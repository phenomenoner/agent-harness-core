#!/usr/bin/env python3
"""Generate the topology explorer from canonical markdown docs.

The markdown docs remain the source of truth. This script extracts the topology
contract tables into a compact graph payload and embeds that payload into a
standalone HTML explorer that can be opened directly from disk.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
TOPOLOGY_DOC = ROOT / "docs" / "agent-harness-topology-contract.md"
OPERATIONS_DOC = ROOT / "docs" / "agent-harness-operations-handbook.md"
OUT_JSON = ROOT / "docs" / "topology-explorer-data.json"
OUT_HTML = ROOT / "docs" / "topology-explorer.html"


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


@dataclass
class Table:
    heading: str
    headers: list[str]
    rows: list[list[str]]


def strip_markdown(value: str) -> str:
    value = value.strip()
    value = re.sub(r"`([^`]+)`", r"\1", value)
    value = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", value)
    value = re.sub(r"\*\*([^*]+)\*\*", r"\1", value)
    return value.strip()


def slug(value: str) -> str:
    out = re.sub(r"[^a-zA-Z0-9]+", "-", strip_markdown(value).lower()).strip("-")
    return out or "node"


def split_table_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def is_separator(line: str) -> bool:
    cells = split_table_row(line)
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell.strip()) for cell in cells)


def parse_tables(markdown: str) -> list[Table]:
    tables: list[Table] = []
    heading = "Document"
    lines = markdown.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("## "):
            heading = line.removeprefix("## ").strip()
        if line.startswith("|") and i + 1 < len(lines) and is_separator(lines[i + 1]):
            headers = split_table_row(line)
            i += 2
            rows: list[list[str]] = []
            while i < len(lines) and lines[i].startswith("|"):
                row = split_table_row(lines[i])
                if len(row) < len(headers):
                    row += [""] * (len(headers) - len(row))
                rows.append(row[: len(headers)])
                i += 1
            tables.append(Table(heading=heading, headers=headers, rows=rows))
            continue
        i += 1
    return tables


def find_table(tables: Iterable[Table], heading: str, first_header: str) -> Table:
    for table in tables:
        if table.heading == heading and table.headers and table.headers[0] == first_header:
            return table
    raise SystemExit(f"missing table: {heading} / {first_header}")


def doc_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def add_node(nodes: dict[str, dict], **node: object) -> str:
    node_id = str(node["id"])
    if node_id in nodes:
        return node_id
    nodes[node_id] = dict(node)
    return node_id


def add_edge(edges: list[dict], source: str, target: str, label: str) -> None:
    edges.append({"source": source, "target": target, "label": label})


def extract_operations_summary(markdown: str) -> str:
    marker = "## Current Live Validation"
    start = markdown.find(marker)
    if start < 0:
        return ""
    body = markdown[start + len(marker) :].strip()
    paragraphs = [p.strip().replace("\n", " ") for p in body.split("\n\n") if p.strip()]
    return paragraphs[0] if paragraphs else ""


def build_journeys(nodes: dict[str, dict]) -> list[dict]:
    """Curated human paths through the generated topology graph."""

    def existing(node_ids: list[str]) -> list[str]:
        return [node_id for node_id in node_ids if node_id in nodes]

    journeys = [
        {
            "id": "turn-lifecycle",
            "title": "Channel Turn Lifecycle",
            "summary": "Follow one Discord or Telegram message from identity binding to queue, prompt, Codex runtime, final outbox, and delivery.",
            "why": "Use this when a turn times out, replies on the wrong channel, or writes progress text into the final answer.",
            "nodes": existing(
                [
                    "axis-platform",
                    "axis-channelid",
                    "axis-agentid",
                    "axis-sessionkey",
                    "component-channel-identity",
                    "component-channel-state",
                    "component-channel-ingress-channel-runtime",
                    "component-runtime-queue-runtime-worker",
                    "component-prompt-turns",
                    "component-codex-runtime",
                    "component-runtime-pipeline",
                    "component-channel-delivery",
                ]
            ),
            "steps": [
                {"title": "Prove the lane", "body": "Confirm platform, account, channel, user, agent, and session before reconnecting prior work."},
                {"title": "Queue the turn", "body": "Ingress writes a source-correlated queue item and keeps runtime class and session freshness visible."},
                {"title": "Assemble prompt", "body": "Prompt files, selected skills, memory, and inbound context are scoped by the resolved agent."},
                {"title": "Run and recover", "body": "Codex runtime records completion, protocol errors, tool-use timeout summaries, and fresh-thread recovery attempts."},
                {"title": "Deliver final", "body": "Runtime pipeline and delivery must emit one final outbox or one terminal notification, separate from progress."},
            ],
        },
        {
            "id": "agent-isolation",
            "title": "Multi-Agent Isolation",
            "summary": "Inspect where agentId must survive so main, Xiaoxiaoli, and future agents do not cross-suppress or cross-pollute state.",
            "why": "Use this when a non-main agent behaves like main, inherits the wrong memory, or loses final delivery because another agent owns shared state.",
            "nodes": existing(
                [
                    "axis-agentid",
                    "axis-sessionkey",
                    "component-channel-state",
                    "component-prompt-turns",
                    "component-runtime-queue-runtime-worker",
                    "component-runtime-pipeline",
                    "component-memory",
                    "gap-multi-agent-full-matrix-gap",
                    "gap-per-agent-memory-recall-compartment-gap",
                ]
            ),
            "steps": [
                {"title": "Agent is a routing boundary", "body": "agentId must survive channel state, prompt assembly, runtime lanes, outbox, delivery, and memory."},
                {"title": "Artifacts are scoped", "body": "Sessions and memory artifacts can be stored under per-agent paths."},
                {"title": "Recall still needs stricter policy", "body": "Xiaoxiaoli currently allows global imported fallback memory, so public/non-main agents need a recall compartment gate."},
                {"title": "Promotion proof", "body": "Run the full multi-agent matrix and a memory recall regression that excludes main private/global imported memories."},
            ],
        },
        {
            "id": "long-task-resilience",
            "title": "Long Task Resilience",
            "summary": "Track the parts that keep a long running task observable without letting progress, tool use, or compaction destroy final delivery.",
            "why": "Use this after idle app-server timeouts, excessive progress edits, missing final replies, or compact/retry confusion.",
            "nodes": existing(
                [
                    "component-runtime-queue-runtime-worker",
                    "component-codex-runtime",
                    "component-progress",
                    "component-runtime-pipeline",
                    "component-channel-delivery",
                    "gap-tool-use-timeout-recovery-gap",
                    "gap-progress-delivery-volume-gap",
                    "gap-progress-final-surface-gap",
                    "gap-virtual-session-continuity-gap",
                ]
            ),
            "steps": [
                {"title": "Keep the queue alive", "body": "Leases and runtime classes prevent one slow task from hiding every other lane."},
                {"title": "Bound external tools", "body": "Round10 adds an initial active tool-use idle guard and one bounded recovery prompt."},
                {"title": "Separate progress from final", "body": "Progress panels can be verbose; final channel replies must stay final-answer only."},
                {"title": "Preserve continuity", "body": "Virtual-session rollover must prove fresh concrete sessions without losing working context or trace."},
            ],
        },
        {
            "id": "memory-ownership",
            "title": "Memory Ownership",
            "summary": "Understand the Store / Pack / Observe boundary, current fallback posture, and what remains before openclaw-mem parity.",
            "why": "Use this when recall quality, graph freshness, Qdrant Edge, or per-agent memory scope looks suspicious.",
            "nodes": existing(
                [
                    "component-memory",
                    "ops-current-live-validation",
                    "gap-openclaw-mem-full-parity-gap",
                    "gap-per-agent-memory-recall-compartment-gap",
                    "gap-repo-code-graph-support-gap",
                ]
            ),
            "steps": [
                {"title": "Read the live posture", "body": "The current live path is Ready through migration fallback while the mem-engine bridge response is absent."},
                {"title": "Keep ownership clean", "body": "Qdrant Edge is preserved snapshot evidence and read-index/cache, not canonical write ownership."},
                {"title": "Fix public-agent recall", "body": "Per-agent artifacts exist; recall must still become agent/channel-policy aware."},
                {"title": "Promote only with receipts", "body": "Bridge, native backend, routeAuto, provenance, and freshness must be green before claiming parity."},
            ],
        },
        {
            "id": "release-review",
            "title": "Release Review Path",
            "summary": "Start from a changed area, jump to docs/tests, then inspect the expected-vs-actual gaps before release or cutover.",
            "why": "Use this before a behavior-changing diff, especially runtime, channel, prompt, delivery, memory, or supervisor work.",
            "nodes": existing(
                [
                    "canon-topology-contract",
                    "ops-current-live-validation",
                    "impact-codex-runtime-or-completion-recording",
                    "impact-final-outbox-or-delivery",
                    "impact-memory-or-graph-recall",
                    "impact-supervisor-or-live-cutover",
                    "gap-scenario-matrix-coverage-gap",
                    "gap-supervisor-service-health-precedence-gap",
                ]
            ),
            "steps": [
                {"title": "Pick the changed area", "body": "The impact matrix gives the minimum docs and scenario pack for a diff."},
                {"title": "Check open gaps", "body": "Do not collapse usable fallback evidence into full design parity."},
                {"title": "Update canon first", "body": "Topology, invariants, release checklist, and operator docs move together when expectations change."},
                {"title": "Regenerate the explorer", "body": "Run the sync command after topology canon changes so the page remains a support-plane reflection."},
            ],
        },
    ]
    return journeys


def build_payload() -> dict:
    topology_md = TOPOLOGY_DOC.read_text(encoding="utf-8")
    operations_md = OPERATIONS_DOC.read_text(encoding="utf-8")
    tables = parse_tables(topology_md)

    axis_table = find_table(tables, "Identity Axes", "Axis")
    owner_table = find_table(tables, "Ownership Boundaries", "Component")
    impact_table = find_table(tables, "Mandatory Impact Matrix", "Changed Area")
    gap_table = find_table(tables, "Expected Vs Actual Gaps", "Area")

    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    add_node(
        nodes,
        id="canon-topology-contract",
        label="Topology Contract",
        group="canon",
        state="canonical",
        summary="Canonical identity axes, ownership boundaries, impact matrix, scenario packs, and expected-vs-actual gaps.",
        refs=[rel(TOPOLOGY_DOC)],
    )
    add_node(
        nodes,
        id="ops-current-live-validation",
        label="Current Live Validation",
        group="canon",
        state="live",
        summary=strip_markdown(extract_operations_summary(operations_md)),
        refs=[rel(OPERATIONS_DOC)],
    )
    add_edge(edges, "ops-current-live-validation", "canon-topology-contract", "live evidence maps to")

    component_ids: list[str] = []
    for row in owner_table.rows:
        component, owns, reads, writes, invariants = row
        node_id = f"component-{slug(component)}"
        component_ids.append(node_id)
        add_node(
            nodes,
            id=node_id,
            label=strip_markdown(component),
            group="component",
            state="implemented",
            summary=strip_markdown(owns),
            reads=strip_markdown(reads),
            writes=strip_markdown(writes),
            invariants=strip_markdown(invariants),
            refs=[rel(TOPOLOGY_DOC)],
        )
        add_edge(edges, "canon-topology-contract", node_id, "owns boundary")

    for row in axis_table.rows:
        axis, meaning, preserved = row
        node_id = f"axis-{slug(axis)}"
        add_node(
            nodes,
            id=node_id,
            label=strip_markdown(axis),
            group="identity",
            state="required",
            summary=strip_markdown(meaning),
            preservedThrough=strip_markdown(preserved),
            refs=[rel(TOPOLOGY_DOC)],
        )
        add_edge(edges, "canon-topology-contract", node_id, "defines")
        preserved_text = preserved.lower()
        for component_id in component_ids:
            component_label = str(nodes[component_id]["label"]).lower()
            tokens = re.split(r"[^a-z0-9]+", component_label)
            if any(token and token in preserved_text for token in tokens):
                add_edge(edges, node_id, component_id, "must survive through")

    for row in impact_table.rows:
        area, review_docs, scenario_pack = row
        node_id = f"impact-{slug(area)}"
        add_node(
            nodes,
            id=node_id,
            label=strip_markdown(area),
            group="gate",
            state="release-gate",
            summary=strip_markdown(scenario_pack),
            reviewDocs=strip_markdown(review_docs),
            refs=[rel(TOPOLOGY_DOC)],
        )
        add_edge(edges, node_id, "canon-topology-contract", "reviewed by")
        area_text = area.lower()
        for component_id in component_ids:
            label = str(nodes[component_id]["label"]).lower()
            if any(token and token in area_text for token in re.split(r"[^a-z0-9]+", label)):
                add_edge(edges, component_id, node_id, "changes trigger")

    for row in gap_table.rows:
        area, target, evidence, gap_label, gate = row
        gap = strip_markdown(gap_label)
        node_id = f"gap-{slug(gap or area)}"
        state = "open-gap"
        if "implemented" in evidence.lower() and "gap" not in gap.lower():
            state = "partial"
        add_node(
            nodes,
            id=node_id,
            label=gap or strip_markdown(area),
            group="gap",
            state=state,
            summary=strip_markdown(area),
            designTarget=strip_markdown(target),
            currentEvidence=strip_markdown(evidence),
            promotionGate=strip_markdown(gate),
            refs=[rel(TOPOLOGY_DOC)],
        )
        add_edge(edges, node_id, "canon-topology-contract", "tracked in")
        gap_text = (area + " " + target + " " + evidence).lower()
        for component_id in component_ids:
            label = str(nodes[component_id]["label"]).lower()
            if any(token and token in gap_text for token in re.split(r"[^a-z0-9]+", label)):
                add_edge(edges, component_id, node_id, "has gap")

    return {
        "schema": "agent-harness.topology-explorer.v1",
        "title": "Agent Harness Topology Explorer",
        "sourceFiles": [
            {"path": rel(TOPOLOGY_DOC), "sha256": doc_hash(TOPOLOGY_DOC)},
            {"path": rel(OPERATIONS_DOC), "sha256": doc_hash(OPERATIONS_DOC)},
        ],
        "syncCommand": "python tools/generate_topology_explorer.py",
        "nodes": list(nodes.values()),
        "edges": edges,
        "journeys": build_journeys(nodes),
    }


def render_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=False, indent=2)
    script_json = (
        data_json.replace("</", "<\\/")
        .replace("<", "\\u003c")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(payload['title'])}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f7fb;
      --ink: #172033;
      --muted: #5b6474;
      --line: #d6dce8;
      --panel: #ffffff;
      --panel-2: #eef3f7;
      --accent: #2563a8;
      --green: #15845c;
      --amber: #9a6700;
      --red: #b13f3f;
      --violet: #7650a8;
      --shadow: 0 18px 44px rgba(19, 33, 56, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Inter", Arial, sans-serif;
      color: var(--ink);
      background: var(--bg);
    }}
    header {{
      padding: 28px 32px 22px;
      background: #182235;
      color: white;
      border-bottom: 1px solid rgba(255,255,255,0.12);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 30px;
      letter-spacing: 0;
    }}
    .lead {{
      max-width: 1180px;
      margin: 0;
      color: #d8e0ec;
      line-height: 1.55;
    }}
    .toolbar {{
      display: grid;
      grid-template-columns: minmax(220px, 1fr) repeat(5, auto);
      gap: 10px;
      padding: 14px 18px;
      background: var(--panel);
      border-bottom: 1px solid var(--line);
      align-items: center;
      position: sticky;
      top: 0;
      z-index: 4;
    }}
    input, button {{
      font: inherit;
      border: 1px solid var(--line);
      background: white;
      color: var(--ink);
      border-radius: 6px;
      min-height: 38px;
    }}
    input {{
      padding: 8px 10px;
      min-width: 0;
    }}
    button {{
      padding: 8px 12px;
      cursor: pointer;
    }}
    button.active {{
      background: #20324f;
      color: white;
      border-color: #20324f;
    }}
    main {{
      display: grid;
      grid-template-columns: minmax(520px, 1fr) 390px;
      min-height: calc(100vh - 142px);
    }}
    .canvas-wrap {{
      position: relative;
      min-height: 680px;
      background:
        linear-gradient(rgba(24,34,53,.04) 1px, transparent 1px),
        linear-gradient(90deg, rgba(24,34,53,.04) 1px, transparent 1px);
      background-size: 32px 32px;
      overflow: hidden;
    }}
    canvas {{
      width: 100%;
      height: 100%;
      display: block;
    }}
    .legend {{
      position: absolute;
      left: 18px;
      bottom: 18px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      max-width: calc(100% - 36px);
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      gap: 7px;
      padding: 6px 9px;
      background: rgba(255,255,255,.88);
      border: 1px solid var(--line);
      border-radius: 999px;
      color: var(--muted);
      font-size: 12px;
      box-shadow: 0 8px 20px rgba(19,33,56,.08);
    }}
    .dot {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
    }}
    aside {{
      border-left: 1px solid var(--line);
      background: var(--panel);
      overflow: auto;
      max-height: calc(100vh - 142px);
    }}
    .panel {{
      padding: 18px;
      border-bottom: 1px solid var(--line);
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }}
    .metric {{
      padding: 10px;
      background: var(--panel-2);
      border: 1px solid var(--line);
      border-radius: 6px;
    }}
    .metric strong {{
      display: block;
      font-size: 22px;
      line-height: 1.1;
    }}
    .metric span {{
      color: var(--muted);
      font-size: 12px;
    }}
    .node-title {{
      margin: 0 0 8px;
      font-size: 20px;
    }}
    .node-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 12px;
    }}
    .kv {{
      margin: 13px 0 0;
    }}
    .kv h3 {{
      margin: 0 0 5px;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0;
    }}
    .kv p, .kv ul {{
      margin: 0;
      line-height: 1.48;
    }}
    .list {{
      display: grid;
      gap: 8px;
    }}
    .list button {{
      width: 100%;
      text-align: left;
      padding: 10px;
      background: white;
    }}
    .list button:hover {{
      border-color: var(--accent);
    }}
    code {{
      background: #eef1f6;
      padding: 1px 4px;
      border-radius: 4px;
    }}
    a {{ color: var(--accent); }}
    @media (max-width: 900px) {{
      .toolbar {{
        grid-template-columns: 1fr 1fr;
      }}
      main {{
        grid-template-columns: 1fr;
      }}
      aside {{
        max-height: none;
        border-left: 0;
        border-top: 1px solid var(--line);
      }}
      .canvas-wrap {{
        min-height: 520px;
      }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>{html.escape(payload['title'])}</h1>
    <p class="lead">Interactive map generated from the canonical topology contract. Use it to inspect identity axes, runtime components, release gates, and design gaps without turning the graph into a second source of truth.</p>
  </header>
  <section class="toolbar" aria-label="Topology explorer controls">
    <input id="search" type="search" placeholder="Search components, gaps, invariants, refs">
    <button type="button" data-filter="all" class="active">All</button>
    <button type="button" data-filter="component">Components</button>
    <button type="button" data-filter="gap">Gaps</button>
    <button type="button" data-filter="gate">Gates</button>
    <button type="button" id="reset">Reset</button>
  </section>
  <main>
    <section class="canvas-wrap" aria-label="Interactive topology canvas">
      <canvas id="graph"></canvas>
      <div id="legend" class="legend"></div>
    </section>
    <aside>
      <section class="panel">
        <div class="metrics" id="metrics"></div>
      </section>
      <section class="panel" id="details"></section>
      <section class="panel">
        <h2 class="node-title">Visible Nodes</h2>
        <div id="nodeList" class="list"></div>
      </section>
    </aside>
  </main>
  <script type="application/json" id="topology-data">{script_json}</script>
  <script>
    const payload = JSON.parse(document.getElementById('topology-data').textContent);
    const nodes = payload.nodes.map((node, index) => ({{
      ...node,
      x: 180 + (index % 7) * 95,
      y: 120 + Math.floor(index / 7) * 78,
      vx: 0,
      vy: 0,
      radius: node.group === 'canon' ? 22 : node.group === 'gap' ? 18 : 15
    }}));
    const byId = new Map(nodes.map(node => [node.id, node]));
    const edges = payload.edges
      .map(edge => ({{ ...edge, sourceNode: byId.get(edge.source), targetNode: byId.get(edge.target) }}))
      .filter(edge => edge.sourceNode && edge.targetNode);
    const colors = {{
      canon: '#20324f',
      component: '#2563a8',
      identity: '#15845c',
      gate: '#9a6700',
      gap: '#b13f3f'
    }};
    const canvas = document.getElementById('graph');
    const ctx = canvas.getContext('2d');
    let scale = 1;
    let panX = 0;
    let panY = 0;
    let selected = nodes[0];
    let filter = 'all';
    let query = '';
    let dragging = null;
    let panning = false;
    let lastPointer = null;

    function visible(node) {{
      const matchesFilter = filter === 'all' || node.group === filter;
      const haystack = JSON.stringify(node).toLowerCase();
      const matchesQuery = !query || haystack.includes(query.toLowerCase());
      return matchesFilter && matchesQuery;
    }}

    function resize() {{
      const rect = canvas.getBoundingClientRect();
      canvas.width = Math.max(640, Math.floor(rect.width * devicePixelRatio));
      canvas.height = Math.max(520, Math.floor(rect.height * devicePixelRatio));
      ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
      draw();
    }}

    function step() {{
      const visibleNodes = nodes.filter(visible);
      for (const edge of edges) {{
        if (!visible(edge.sourceNode) || !visible(edge.targetNode)) continue;
        const dx = edge.targetNode.x - edge.sourceNode.x;
        const dy = edge.targetNode.y - edge.sourceNode.y;
        const dist = Math.max(1, Math.hypot(dx, dy));
        const desired = edge.sourceNode.group === 'canon' ? 170 : 130;
        const force = (dist - desired) * 0.003;
        const fx = (dx / dist) * force;
        const fy = (dy / dist) * force;
        edge.sourceNode.vx += fx;
        edge.sourceNode.vy += fy;
        edge.targetNode.vx -= fx;
        edge.targetNode.vy -= fy;
      }}
      for (let i = 0; i < visibleNodes.length; i++) {{
        for (let j = i + 1; j < visibleNodes.length; j++) {{
          const a = visibleNodes[i];
          const b = visibleNodes[j];
          const dx = b.x - a.x;
          const dy = b.y - a.y;
          const dist = Math.max(1, Math.hypot(dx, dy));
          const force = Math.min(2.8, 800 / (dist * dist));
          const fx = (dx / dist) * force;
          const fy = (dy / dist) * force;
          a.vx -= fx;
          a.vy -= fy;
          b.vx += fx;
          b.vy += fy;
        }}
      }}
      for (const node of visibleNodes) {{
        if (dragging === node) continue;
        node.vx += ((canvas.clientWidth / 2 - panX) / scale - node.x) * 0.0008;
        node.vy += ((canvas.clientHeight / 2 - panY) / scale - node.y) * 0.0008;
        node.vx *= 0.82;
        node.vy *= 0.82;
        node.x += node.vx;
        node.y += node.vy;
      }}
      draw();
      requestAnimationFrame(step);
    }}

    function draw() {{
      ctx.save();
      ctx.clearRect(0, 0, canvas.clientWidth, canvas.clientHeight);
      ctx.translate(panX, panY);
      ctx.scale(scale, scale);
      ctx.lineWidth = 1.2 / scale;
      ctx.strokeStyle = 'rgba(91,100,116,.28)';
      for (const edge of edges) {{
        if (!visible(edge.sourceNode) || !visible(edge.targetNode)) continue;
        ctx.beginPath();
        ctx.moveTo(edge.sourceNode.x, edge.sourceNode.y);
        ctx.lineTo(edge.targetNode.x, edge.targetNode.y);
        ctx.stroke();
      }}
      for (const node of nodes) {{
        if (!visible(node)) continue;
        const color = colors[node.group] || '#5b6474';
        ctx.beginPath();
        ctx.fillStyle = color;
        ctx.strokeStyle = node === selected ? '#111827' : 'white';
        ctx.lineWidth = node === selected ? 4 / scale : 2 / scale;
        ctx.arc(node.x, node.y, node.radius, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
        ctx.fillStyle = '#172033';
        ctx.font = `${{Math.max(10, 12 / scale)}}px Segoe UI, Arial`;
        ctx.textAlign = 'center';
        ctx.fillText(node.label.length > 28 ? node.label.slice(0, 25) + '...' : node.label, node.x, node.y + node.radius + 14 / scale);
      }}
      ctx.restore();
    }}

    function screenToWorld(event) {{
      const rect = canvas.getBoundingClientRect();
      return {{
        x: (event.clientX - rect.left - panX) / scale,
        y: (event.clientY - rect.top - panY) / scale
      }};
    }}

    function hit(event) {{
      const p = screenToWorld(event);
      return [...nodes].reverse().find(node => visible(node) && Math.hypot(node.x - p.x, node.y - p.y) <= node.radius + 8);
    }}

    function select(node) {{
      selected = node;
      renderDetails();
      renderList();
      draw();
    }}

    function renderMetrics() {{
      const counts = nodes.reduce((acc, node) => {{
        acc[node.group] = (acc[node.group] || 0) + 1;
        return acc;
      }}, {{}});
      document.getElementById('metrics').innerHTML = [
        ['Nodes', nodes.length],
        ['Edges', edges.length],
        ['Components', counts.component || 0],
        ['Open gaps', counts.gap || 0]
      ].map(([label, value]) => `<div class="metric"><strong>${{value}}</strong><span>${{label}}</span></div>`).join('');
    }}

    function escapeText(value) {{
      return String(value || '').replace(/[&<>"']/g, ch => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[ch]));
    }}

    function renderDetails() {{
      const node = selected || nodes[0];
      const refs = (node.refs || []).map(ref => `<li><a href="${{escapeText(ref)}}">${{escapeText(ref)}}</a></li>`).join('');
      const fields = [
        ['Summary', node.summary],
        ['Design Target', node.designTarget],
        ['Current Evidence', node.currentEvidence],
        ['Promotion Gate', node.promotionGate],
        ['Reads', node.reads],
        ['Writes', node.writes],
        ['Invariants', node.invariants],
        ['Review Docs', node.reviewDocs],
        ['Preserved Through', node.preservedThrough]
      ].filter(([, value]) => value);
      const related = edges
        .filter(edge => edge.sourceNode === node || edge.targetNode === node)
        .map(edge => {{
          const other = edge.sourceNode === node ? edge.targetNode : edge.sourceNode;
          return `<li><button type="button" data-node="${{escapeText(other.id)}}">${{escapeText(edge.label)}}: ${{escapeText(other.label)}}</button></li>`;
        }}).join('');
      document.getElementById('details').innerHTML = `
        <h2 class="node-title">${{escapeText(node.label)}}</h2>
        <div class="node-meta"><span class="pill"><span class="dot" style="background:${{colors[node.group] || '#5b6474'}}"></span>${{escapeText(node.group)}}</span><span class="pill">${{escapeText(node.state)}}</span></div>
        ${{fields.map(([label, value]) => `<div class="kv"><h3>${{escapeText(label)}}</h3><p>${{escapeText(value)}}</p></div>`).join('')}}
        <div class="kv"><h3>Refs</h3><ul>${{refs}}</ul></div>
        <div class="kv"><h3>Relationships</h3><ul>${{related}}</ul></div>
      `;
      document.querySelectorAll('#details [data-node]').forEach(button => {{
        button.addEventListener('click', () => select(byId.get(button.dataset.node)));
      }});
    }}

    function renderList() {{
      const list = nodes.filter(visible).sort((a, b) => a.group.localeCompare(b.group) || a.label.localeCompare(b.label));
      document.getElementById('nodeList').innerHTML = list
        .map(node => `<button type="button" data-node="${{escapeText(node.id)}}">${{escapeText(node.label)}}<br><small>${{escapeText(node.group)}} / ${{escapeText(node.state)}}</small></button>`)
        .join('');
      document.querySelectorAll('#nodeList [data-node]').forEach(button => {{
        button.addEventListener('click', () => select(byId.get(button.dataset.node)));
      }});
    }}

    function renderLegend() {{
      document.getElementById('legend').innerHTML = Object.entries(colors)
        .map(([group, color]) => `<span class="pill"><span class="dot" style="background:${{color}}"></span>${{group}}</span>`)
        .join('');
    }}

    document.getElementById('search').addEventListener('input', event => {{
      query = event.target.value;
      renderList();
      draw();
    }});
    document.querySelectorAll('[data-filter]').forEach(button => {{
      button.addEventListener('click', () => {{
        filter = button.dataset.filter;
        document.querySelectorAll('[data-filter]').forEach(b => b.classList.toggle('active', b === button));
        renderList();
        draw();
      }});
    }});
    document.getElementById('reset').addEventListener('click', () => {{
      scale = 1;
      panX = 0;
      panY = 0;
      query = '';
      document.getElementById('search').value = '';
      filter = 'all';
      document.querySelectorAll('[data-filter]').forEach(b => b.classList.toggle('active', b.dataset.filter === 'all'));
      renderList();
      draw();
    }});
    canvas.addEventListener('pointerdown', event => {{
      const node = hit(event);
      lastPointer = {{ x: event.clientX, y: event.clientY }};
      if (node) {{
        dragging = node;
        select(node);
      }} else {{
        panning = true;
      }}
      canvas.setPointerCapture(event.pointerId);
    }});
    canvas.addEventListener('pointermove', event => {{
      if (!lastPointer) return;
      if (dragging) {{
        const p = screenToWorld(event);
        dragging.x = p.x;
        dragging.y = p.y;
        dragging.vx = 0;
        dragging.vy = 0;
      }} else if (panning) {{
        panX += event.clientX - lastPointer.x;
        panY += event.clientY - lastPointer.y;
      }}
      lastPointer = {{ x: event.clientX, y: event.clientY }};
      draw();
    }});
    canvas.addEventListener('pointerup', event => {{
      dragging = null;
      panning = false;
      lastPointer = null;
      canvas.releasePointerCapture(event.pointerId);
    }});
    canvas.addEventListener('wheel', event => {{
      event.preventDefault();
      const delta = event.deltaY < 0 ? 1.08 : 0.92;
      scale = Math.max(0.45, Math.min(2.4, scale * delta));
      draw();
    }}, {{ passive: false }});
    addEventListener('resize', resize);
    renderMetrics();
    renderDetails();
    renderList();
    renderLegend();
    resize();
    requestAnimationFrame(step);
  </script>
</body>
</html>
"""


def render_guided_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=False, indent=2)
    script_json = (
        data_json.replace("</", "<\\/")
        .replace("<", "\\u003c")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )
    template = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__TITLE__</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4f6f8;
      --ink: #172033;
      --muted: #5b6474;
      --line: #d6dce8;
      --panel: #ffffff;
      --soft: #eef3f7;
      --accent: #2563a8;
      --green: #15845c;
      --amber: #9a6700;
      --red: #b13f3f;
      --shadow: 0 18px 44px rgba(19,33,56,.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: "Segoe UI", "Inter", Arial, sans-serif;
    }
    header {
      padding: 24px 30px 18px;
      background: #182235;
      color: #fff;
      border-bottom: 1px solid rgba(255,255,255,.12);
    }
    h1 { margin: 0 0 8px; font-size: 28px; letter-spacing: 0; }
    .lead { max-width: 1180px; margin: 0; color: #d8e0ec; line-height: 1.55; }
    .sources { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; font-size: 12px; color: #d8e0ec; }
    .sources code { color: #fff; background: rgba(255,255,255,.12); }
    .toolbar {
      position: sticky;
      top: 0;
      z-index: 5;
      display: grid;
      grid-template-columns: minmax(260px, 1fr) repeat(5, auto);
      gap: 10px;
      padding: 14px 18px;
      background: var(--panel);
      border-bottom: 1px solid var(--line);
      align-items: center;
    }
    input, button {
      font: inherit;
      min-height: 38px;
      border: 1px solid var(--line);
      border-radius: 7px;
      background: #fff;
      color: var(--ink);
    }
    input { min-width: 0; padding: 8px 10px; }
    button { padding: 8px 12px; cursor: pointer; }
    button.active { background: #20324f; color: #fff; border-color: #20324f; }
    main {
      display: grid;
      grid-template-columns: 360px minmax(560px, 1fr) 420px;
      min-height: calc(100vh - 142px);
    }
    .left-rail, aside {
      background: var(--panel);
      overflow: auto;
      max-height: calc(100vh - 142px);
    }
    .left-rail { border-right: 1px solid var(--line); }
    aside { border-left: 1px solid var(--line); }
    .panel { padding: 18px; border-bottom: 1px solid var(--line); }
    .panel h2 { margin: 0 0 12px; font-size: 18px; letter-spacing: 0; }
    .journey-card {
      width: 100%;
      text-align: left;
      margin: 0 0 10px;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
    }
    .journey-card.active { border-color: var(--accent); box-shadow: inset 4px 0 0 var(--accent); }
    .journey-card strong { display: block; margin-bottom: 5px; }
    .journey-card span { display: block; color: var(--muted); line-height: 1.4; font-size: 13px; }
    .stepper { display: grid; grid-template-columns: 42px 1fr 42px; gap: 8px; margin-top: 12px; align-items: stretch; }
    .stepbox { min-height: 124px; padding: 12px; background: var(--soft); border: 1px solid var(--line); border-radius: 8px; }
    .stepbox .count { color: var(--muted); font-size: 12px; margin-bottom: 6px; }
    .stepbox strong { display: block; margin-bottom: 6px; }
    .stepbox p { margin: 0; line-height: 1.5; }
    .canvas-wrap {
      position: relative;
      min-height: 700px;
      overflow: hidden;
      background:
        linear-gradient(rgba(24,34,53,.045) 1px, transparent 1px),
        linear-gradient(90deg, rgba(24,34,53,.045) 1px, transparent 1px);
      background-size: 32px 32px;
    }
    .canvas-note {
      position: absolute;
      left: 18px;
      top: 18px;
      max-width: 520px;
      padding: 12px 14px;
      background: rgba(255,255,255,.92);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 8px 26px rgba(19,33,56,.08);
    }
    .canvas-note strong { display: block; margin-bottom: 4px; }
    .canvas-note p { margin: 0; color: var(--muted); line-height: 1.45; }
    canvas { width: 100%; height: 100%; display: block; }
    .legend {
      position: absolute;
      left: 18px;
      bottom: 18px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      max-width: calc(100% - 36px);
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      padding: 6px 9px;
      background: rgba(255,255,255,.9);
      border: 1px solid var(--line);
      border-radius: 999px;
      color: var(--muted);
      font-size: 12px;
      box-shadow: 0 8px 20px rgba(19,33,56,.08);
    }
    .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
    .metrics { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
    .metric { padding: 10px; background: var(--soft); border: 1px solid var(--line); border-radius: 6px; }
    .metric strong { display: block; font-size: 22px; line-height: 1.1; }
    .metric span, small { color: var(--muted); font-size: 12px; }
    .node-title { margin: 0 0 8px; font-size: 20px; }
    .node-meta { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 12px; }
    .kv { margin: 13px 0 0; }
    .kv h3, .node-list-section h3 {
      margin: 0 0 5px;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .kv p, .kv ul { margin: 0; line-height: 1.48; }
    .list, .focus-list { display: grid; gap: 8px; }
    .list button, .focus-list button { width: 100%; text-align: left; padding: 10px; background: #fff; }
    .list button:hover, .focus-list button:hover { border-color: var(--accent); }
    .focus-list button.active { background: #20324f; color: #fff; border-color: #20324f; }
    .gap-list button { border-left: 4px solid var(--red); }
    .node-list-section { margin-top: 16px; }
    code { background: #eef1f6; padding: 1px 4px; border-radius: 4px; }
    a { color: var(--accent); }
    @media (max-width: 1180px) {
      .toolbar { grid-template-columns: 1fr 1fr; }
      main { grid-template-columns: 1fr; }
      .left-rail, aside { max-height: none; border: 0; border-bottom: 1px solid var(--line); }
      .canvas-wrap { min-height: 540px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>__TITLE__</h1>
    <p class="lead">Guided topology surface generated from the canonical docs. Start with a journey, walk the steps, then use the canvas and gap queue for drill-down. The page is a support-plane view, not a second source of truth.</p>
    <div class="sources" id="sources"></div>
  </header>
  <section class="toolbar" aria-label="Topology explorer controls">
    <input id="search" type="search" placeholder="Search components, gaps, invariants, refs">
    <button type="button" data-filter="all" class="active">All</button>
    <button type="button" data-filter="component">Components</button>
    <button type="button" data-filter="gap">Gaps</button>
    <button type="button" data-filter="gate">Gates</button>
    <button type="button" id="reset">Reset</button>
  </section>
  <main>
    <nav class="left-rail" aria-label="Guided topology journeys">
      <section class="panel">
        <h2>Guided Paths</h2>
        <div id="journeyList"></div>
      </section>
      <section class="panel">
        <h2>Current Step</h2>
        <div id="stepper"></div>
      </section>
      <section class="panel">
        <h2>Path Focus</h2>
        <div id="focusList" class="focus-list"></div>
      </section>
    </nav>
    <section class="canvas-wrap" aria-label="Interactive topology canvas">
      <div class="canvas-note" id="canvasNote"></div>
      <canvas id="graph"></canvas>
      <div id="legend" class="legend"></div>
    </section>
    <aside>
      <section class="panel"><div class="metrics" id="metrics"></div></section>
      <section class="panel" id="details"></section>
      <section class="panel">
        <h2>Open Gap Queue</h2>
        <div id="gapList" class="list gap-list"></div>
      </section>
      <section class="panel">
        <h2>Visible Nodes</h2>
        <div id="nodeList" class="list"></div>
      </section>
    </aside>
  </main>
  <script type="application/json" id="topology-data">__DATA__</script>
  <script>
    const payload = JSON.parse(document.getElementById('topology-data').textContent);
    const colors = { canon: '#20324f', component: '#2563a8', identity: '#15845c', gate: '#9a6700', gap: '#b13f3f' };
    const columns = { canon: 130, identity: 300, component: 540, gate: 800, gap: 1060 };
    const rows = { canon: 120, identity: 120, component: 120, gate: 140, gap: 120 };
    const seen = {};
    const nodes = payload.nodes.map(node => ({
      ...node,
      x: columns[node.group] || 540,
      y: (rows[node.group] || 120) + ((seen[node.group] = (seen[node.group] || 0) + 1) - 1) * 76,
      vx: 0,
      vy: 0,
      radius: node.group === 'canon' ? 22 : node.group === 'gap' ? 18 : 15
    }));
    const byId = new Map(nodes.map(node => [node.id, node]));
    const edges = payload.edges
      .map(edge => ({ ...edge, sourceNode: byId.get(edge.source), targetNode: byId.get(edge.target) }))
      .filter(edge => edge.sourceNode && edge.targetNode);
    const journeys = payload.journeys || [];
    const canvas = document.getElementById('graph');
    const ctx = canvas.getContext('2d');
    let activeJourney = journeys[0] || null;
    let activeStep = 0;
    let selected = activeJourney ? byId.get(activeJourney.nodes[0]) || nodes[0] : nodes[0];
    let filter = 'all';
    let query = '';
    let scale = 1;
    let panX = 0;
    let panY = 0;
    let dragging = null;
    let panning = false;
    let lastPointer = null;

    function escapeText(value) {
      return String(value || '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    }
    function journeySet() { return new Set(activeJourney ? activeJourney.nodes || [] : []); }
    function activeStepNodeId() {
      if (!activeJourney || !activeJourney.nodes.length) return null;
      return activeJourney.nodes[Math.min(activeStep, activeJourney.nodes.length - 1)];
    }
    function visible(node) {
      const matchesFilter = filter === 'all' || node.group === filter;
      const matchesQuery = !query || JSON.stringify(node).toLowerCase().includes(query.toLowerCase());
      return matchesFilter && matchesQuery;
    }
    function resize() {
      const rect = canvas.getBoundingClientRect();
      canvas.width = Math.max(640, Math.floor(rect.width * devicePixelRatio));
      canvas.height = Math.max(520, Math.floor(rect.height * devicePixelRatio));
      ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
      focusJourney();
    }
    function focusJourney() {
      if (!activeJourney || !activeJourney.nodes.length || !canvas.clientWidth) { draw(); return; }
      const selectedNodes = activeJourney.nodes.map(id => byId.get(id)).filter(Boolean);
      if (!selectedNodes.length) { draw(); return; }
      const minX = Math.min(...selectedNodes.map(n => n.x));
      const maxX = Math.max(...selectedNodes.map(n => n.x));
      const minY = Math.min(...selectedNodes.map(n => n.y));
      const maxY = Math.max(...selectedNodes.map(n => n.y));
      scale = Math.max(.46, Math.min(1.25, Math.min((canvas.clientWidth - 120) / Math.max(1, maxX - minX), (canvas.clientHeight - 160) / Math.max(1, maxY - minY))));
      panX = canvas.clientWidth / 2 - ((minX + maxX) / 2) * scale;
      panY = canvas.clientHeight / 2 - ((minY + maxY) / 2) * scale;
      draw();
    }
    function screenToWorld(event) {
      const rect = canvas.getBoundingClientRect();
      return { x: (event.clientX - rect.left - panX) / scale, y: (event.clientY - rect.top - panY) / scale };
    }
    function hit(event) {
      const p = screenToWorld(event);
      return [...nodes].reverse().find(node => visible(node) && Math.hypot(node.x - p.x, node.y - p.y) <= node.radius + 8);
    }
    function draw() {
      if (!canvas.clientWidth) return;
      const activeIds = journeySet();
      const stepId = activeStepNodeId();
      ctx.save();
      ctx.clearRect(0, 0, canvas.clientWidth, canvas.clientHeight);
      ctx.translate(panX, panY);
      ctx.scale(scale, scale);
      for (const edge of edges) {
        if (!visible(edge.sourceNode) || !visible(edge.targetNode)) continue;
        const inPath = activeIds.has(edge.source) && activeIds.has(edge.target);
        ctx.beginPath();
        ctx.strokeStyle = inPath ? 'rgba(37,99,168,.72)' : 'rgba(91,100,116,.24)';
        ctx.lineWidth = inPath ? 2.5 / scale : 1.1 / scale;
        ctx.moveTo(edge.sourceNode.x, edge.sourceNode.y);
        ctx.lineTo(edge.targetNode.x, edge.targetNode.y);
        ctx.stroke();
      }
      for (const node of nodes) {
        if (!visible(node)) continue;
        const inPath = activeIds.has(node.id);
        const isStep = stepId === node.id;
        ctx.globalAlpha = activeJourney && !inPath ? .34 : 1;
        ctx.beginPath();
        ctx.fillStyle = colors[node.group] || '#5b6474';
        ctx.strokeStyle = isStep ? '#f59e0b' : node === selected ? '#111827' : '#fff';
        ctx.lineWidth = isStep ? 5 / scale : node === selected ? 4 / scale : 2 / scale;
        ctx.arc(node.x, node.y, isStep ? node.radius + 4 : node.radius, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
        ctx.globalAlpha = 1;
        ctx.fillStyle = '#172033';
        ctx.font = `${Math.max(10, 12 / scale)}px Segoe UI, Arial`;
        ctx.textAlign = 'center';
        ctx.fillText(node.label.length > 28 ? node.label.slice(0, 25) + '...' : node.label, node.x, node.y + node.radius + 14 / scale);
      }
      ctx.restore();
    }
    function settle() {
      for (const node of nodes.filter(visible)) {
        if (dragging === node) continue;
        const targetX = columns[node.group] || 540;
        node.vx += (targetX - node.x) * .0009;
        node.vx *= .82;
        node.vy *= .82;
        node.x += node.vx;
        node.y += node.vy;
      }
      draw();
      requestAnimationFrame(settle);
    }
    function select(node) {
      if (!node) return;
      selected = node;
      renderAll(false);
    }
    function renderSources() {
      document.getElementById('sources').innerHTML = [
        `<span>Sync: <code>${escapeText(payload.syncCommand)}</code></span>`,
        ...payload.sourceFiles.map(source => `<span><code>${escapeText(source.path)}</code></span>`)
      ].join('');
    }
    function renderMetrics() {
      const counts = nodes.reduce((acc, node) => { acc[node.group] = (acc[node.group] || 0) + 1; return acc; }, {});
      document.getElementById('metrics').innerHTML = [
        ['Nodes', nodes.length],
        ['Edges', edges.length],
        ['Guided paths', journeys.length],
        ['Open gaps', counts.gap || 0]
      ].map(([label, value]) => `<div class="metric"><strong>${value}</strong><span>${label}</span></div>`).join('');
    }
    function renderJourneyList() {
      document.getElementById('journeyList').innerHTML = journeys.map(journey => `
        <button type="button" class="journey-card ${activeJourney && activeJourney.id === journey.id ? 'active' : ''}" data-journey="${escapeText(journey.id)}">
          <strong>${escapeText(journey.title)}</strong>
          <span>${escapeText(journey.summary)}</span>
        </button>
      `).join('');
      document.querySelectorAll('[data-journey]').forEach(button => button.addEventListener('click', () => {
        activeJourney = journeys.find(journey => journey.id === button.dataset.journey) || journeys[0];
        activeStep = 0;
        selected = byId.get(activeStepNodeId()) || selected;
        renderAll(true);
      }));
    }
    function renderStepper() {
      if (!activeJourney) { document.getElementById('stepper').innerHTML = '<p>No guided paths available.</p>'; return; }
      const steps = activeJourney.steps || [];
      const step = steps[Math.min(activeStep, steps.length - 1)] || { title: activeJourney.title, body: activeJourney.summary };
      document.getElementById('stepper').innerHTML = `
        <div class="kv"><h3>Why this path matters</h3><p>${escapeText(activeJourney.why || activeJourney.summary)}</p></div>
        <div class="stepper">
          <button type="button" id="prevStep" aria-label="Previous step">‹</button>
          <div class="stepbox">
            <div class="count">Step ${Math.min(activeStep + 1, steps.length || 1)} / ${steps.length || 1}</div>
            <strong>${escapeText(step.title)}</strong>
            <p>${escapeText(step.body)}</p>
          </div>
          <button type="button" id="nextStep" aria-label="Next step">›</button>
        </div>
      `;
      document.getElementById('prevStep').addEventListener('click', () => {
        activeStep = Math.max(0, activeStep - 1);
        selected = byId.get(activeStepNodeId()) || selected;
        renderAll(false);
      });
      document.getElementById('nextStep').addEventListener('click', () => {
        activeStep = Math.min(Math.max(0, steps.length - 1), activeStep + 1);
        selected = byId.get(activeStepNodeId()) || selected;
        renderAll(false);
      });
    }
    function renderFocusList() {
      const ids = activeJourney ? activeJourney.nodes || [] : [];
      document.getElementById('focusList').innerHTML = ids.map((id, index) => {
        const node = byId.get(id);
        if (!node) return '';
        return `<button type="button" data-focus-node="${escapeText(id)}" class="${selected && selected.id === id ? 'active' : ''}">${index + 1}. ${escapeText(node.label)}<br><small>${escapeText(node.group)} / ${escapeText(node.state)}</small></button>`;
      }).join('');
      document.querySelectorAll('[data-focus-node]').forEach(button => button.addEventListener('click', () => {
        activeStep = Math.max(0, ids.indexOf(button.dataset.focusNode));
        select(byId.get(button.dataset.focusNode));
      }));
    }
    function renderCanvasNote() {
      if (!activeJourney) return;
      document.getElementById('canvasNote').innerHTML = `<strong>${escapeText(activeJourney.title)}</strong><p>${escapeText(activeJourney.summary)}</p>`;
    }
    function renderDetails() {
      const node = selected || nodes[0];
      const refs = (node.refs || []).map(ref => `<li><a href="${escapeText(ref)}">${escapeText(ref)}</a></li>`).join('');
      const fields = [
        ['Summary', node.summary],
        ['Design Target', node.designTarget],
        ['Current Evidence', node.currentEvidence],
        ['Promotion Gate', node.promotionGate],
        ['Reads', node.reads],
        ['Writes', node.writes],
        ['Invariants', node.invariants],
        ['Review Docs', node.reviewDocs],
        ['Preserved Through', node.preservedThrough]
      ].filter(([, value]) => value);
      const related = edges
        .filter(edge => edge.sourceNode === node || edge.targetNode === node)
        .slice(0, 18)
        .map(edge => {
          const other = edge.sourceNode === node ? edge.targetNode : edge.sourceNode;
          return `<li><button type="button" data-node="${escapeText(other.id)}">${escapeText(edge.label)}: ${escapeText(other.label)}</button></li>`;
        }).join('');
      document.getElementById('details').innerHTML = `
        <h2 class="node-title">${escapeText(node.label)}</h2>
        <div class="node-meta"><span class="pill"><span class="dot" style="background:${colors[node.group] || '#5b6474'}"></span>${escapeText(node.group)}</span><span class="pill">${escapeText(node.state)}</span></div>
        ${fields.map(([label, value]) => `<div class="kv"><h3>${escapeText(label)}</h3><p>${escapeText(value)}</p></div>`).join('')}
        <div class="kv"><h3>Refs</h3><ul>${refs}</ul></div>
        <div class="kv"><h3>Relationships</h3><ul>${related}</ul></div>
      `;
      document.querySelectorAll('#details [data-node]').forEach(button => button.addEventListener('click', () => select(byId.get(button.dataset.node))));
    }
    function renderGapList() {
      const gaps = nodes.filter(node => node.group === 'gap').sort((a, b) => a.label.localeCompare(b.label));
      document.getElementById('gapList').innerHTML = gaps.map(node => `
        <button type="button" data-node="${escapeText(node.id)}">${escapeText(node.label)}<br><small>${escapeText(node.summary)}</small></button>
      `).join('');
      document.querySelectorAll('#gapList [data-node]').forEach(button => button.addEventListener('click', () => select(byId.get(button.dataset.node))));
    }
    function renderList() {
      const list = nodes.filter(visible).sort((a, b) => a.group.localeCompare(b.group) || a.label.localeCompare(b.label));
      const groups = [...new Set(list.map(node => node.group))];
      document.getElementById('nodeList').innerHTML = groups.map(group => `
        <div class="node-list-section">
          <h3>${escapeText(group)}</h3>
          <div class="list">${list.filter(node => node.group === group).map(node => `<button type="button" data-node="${escapeText(node.id)}">${escapeText(node.label)}<br><small>${escapeText(node.state)}</small></button>`).join('')}</div>
        </div>
      `).join('');
      document.querySelectorAll('#nodeList [data-node]').forEach(button => button.addEventListener('click', () => select(byId.get(button.dataset.node))));
    }
    function renderLegend() {
      document.getElementById('legend').innerHTML = Object.entries(colors)
        .map(([group, color]) => `<span class="pill"><span class="dot" style="background:${color}"></span>${group}</span>`)
        .join('');
    }
    function renderAll(refocus) {
      renderJourneyList();
      renderStepper();
      renderFocusList();
      renderCanvasNote();
      renderMetrics();
      renderDetails();
      renderGapList();
      renderList();
      renderLegend();
      if (refocus) focusJourney(); else draw();
    }
    document.getElementById('search').addEventListener('input', event => { query = event.target.value; renderList(); draw(); });
    document.querySelectorAll('[data-filter]').forEach(button => button.addEventListener('click', () => {
      filter = button.dataset.filter;
      document.querySelectorAll('[data-filter]').forEach(b => b.classList.toggle('active', b === button));
      renderList();
      draw();
    }));
    document.getElementById('reset').addEventListener('click', () => {
      scale = 1; panX = 0; panY = 0; query = ''; filter = 'all';
      document.getElementById('search').value = '';
      document.querySelectorAll('[data-filter]').forEach(b => b.classList.toggle('active', b.dataset.filter === 'all'));
      renderAll(true);
    });
    canvas.addEventListener('pointerdown', event => {
      const node = hit(event);
      lastPointer = { x: event.clientX, y: event.clientY };
      if (node) { dragging = node; select(node); } else { panning = true; }
      canvas.setPointerCapture(event.pointerId);
    });
    canvas.addEventListener('pointermove', event => {
      if (!lastPointer) return;
      if (dragging) {
        const p = screenToWorld(event);
        dragging.x = p.x; dragging.y = p.y; dragging.vx = 0; dragging.vy = 0;
      } else if (panning) {
        panX += event.clientX - lastPointer.x;
        panY += event.clientY - lastPointer.y;
      }
      lastPointer = { x: event.clientX, y: event.clientY };
      draw();
    });
    canvas.addEventListener('pointerup', event => {
      dragging = null; panning = false; lastPointer = null; canvas.releasePointerCapture(event.pointerId);
    });
    canvas.addEventListener('wheel', event => {
      event.preventDefault();
      scale = Math.max(.45, Math.min(2.4, scale * (event.deltaY < 0 ? 1.08 : .92)));
      draw();
    }, { passive: false });
    addEventListener('resize', resize);
    renderSources();
    renderAll(false);
    resize();
    requestAnimationFrame(settle);
  </script>
</body>
</html>
"""
    return (
        template.replace("__TITLE__", html.escape(payload["title"]))
        .replace("__DATA__", script_json)
    )


def main() -> int:
    payload = build_payload()
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_HTML.write_text(render_guided_html(payload), encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": True,
                "html": str(OUT_HTML.relative_to(ROOT)),
                "json": str(OUT_JSON.relative_to(ROOT)),
                "nodes": len(payload["nodes"]),
                "edges": len(payload["edges"]),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
