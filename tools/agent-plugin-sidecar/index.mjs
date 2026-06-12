#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import readline from "node:readline";

const PROBE_SCHEMA = "agent-harness.plugin-sidecar-probe.v1";
const PROBE_RECEIPT_SCHEMA = "agent-harness.plugin-sidecar-probe-receipt.v1";
const CATALOG_SCHEMA = "agent-harness.plugin-sidecar-catalog.v1";
const EXECUTION_RECEIPT_SCHEMA = "agent-harness.plugin-sidecar-execution-receipt.v1";
const HOOK_RECEIPT_SCHEMA = "agent-harness.plugin-sidecar-hook-receipt.v1";
const MEMORY_SLOT_RECEIPT_SCHEMA = "agent-harness.plugin-sidecar-memory-slot-receipt.v1";
const OPENCLAW_MEM_SERVICE_RECEIPT_SCHEMA = "agent-harness.openclaw-mem-sidecar-receipt.v1";

function parseArgs(argv) {
  const args = {
    harnessHome: process.env.AGENT_HARNESS_HOME || ".agent-harness",
    probe: false,
    stdio: false,
    writeReceipt: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const flag = argv[i];
    if (flag === "--harness-home" || flag === "--target-home") {
      i += 1;
      args.harnessHome = requiredValue(argv, i, flag);
    } else if (flag === "--probe") {
      args.probe = true;
    } else if (flag === "--stdio") {
      args.stdio = true;
    } else if (flag === "--write-receipt") {
      args.writeReceipt = true;
    } else {
      throw new Error(`unknown argument: ${flag}`);
    }
  }
  return args;
}

function requiredValue(argv, index, flag) {
  const value = argv[index];
  if (!value) {
    throw new Error(`${flag} requires a value`);
  }
  return value;
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function tryReadJson(file) {
  try {
    return { value: readJson(file), error: null };
  } catch (error) {
    return { value: null, error };
  }
}

function buildProbe(harnessHome) {
  const registryFile = path.join(harnessHome, "state", "harness-registry.json");
  const installsFile = path.join(harnessHome, "plugins", "installs.json");
  const warnings = [];
  const registryRead = tryReadJson(registryFile);
  const installsRead = tryReadJson(installsFile);
  const registry = registryRead.value;
  const installs = installsRead.value;

  if (registryRead.error) {
    warnings.push(`registry unavailable: ${registryRead.error.message}`);
  }
  if (installsRead.error) {
    warnings.push(`plugin install manifest unavailable: ${installsRead.error.message}`);
  }

  const plugins = Array.isArray(registry?.plugins) ? registry.plugins : [];
  const sidecarRequired = plugins
    .filter((plugin) => plugin.sidecarRequired === true)
    .map((plugin) => ({
      id: String(plugin.id || ""),
      source: plugin.source || null,
      enabled: plugin.enabled ?? null,
      memoryRelated: plugin.memoryRelated === true,
      channelRelated: plugin.channelRelated === true,
    }));
  const notSidecarRequired = plugins
    .filter((plugin) => plugin.sidecarRequired === false)
    .map((plugin) => String(plugin.id || ""))
    .filter(Boolean);
  const catalog = buildCatalogFromData(harnessHome, registry, installs);
  const status = registry ? "contract-ready" : "blocked";

  return {
    schema: PROBE_SCHEMA,
    status,
    harnessHome,
    registryFile,
    installsFile,
    installsPresent: Boolean(installs),
    pid: process.pid,
    node: process.version,
    capabilities: [
      "sidecar.status",
      "plugins.list",
      "tools.list",
      "tools.probe",
      "tools.call",
      "hooks.invoke",
      "memory.slot",
      "openclaw_mem.status",
      "openclaw_mem.recall",
      "openclaw_mem.propose",
      "openclaw_mem.store",
    ],
    summary: {
      plugins: plugins.length,
      sidecarRequired: sidecarRequired.length,
      notSidecarRequired: notSidecarRequired.length,
      resolvedManifests: catalog.summary.resolvedManifests,
      unresolvedSidecarRequired: catalog.summary.unresolvedSidecarRequired,
      tools: catalog.tools.length,
    },
    sidecarRequired,
    notSidecarRequired,
    limitations: [
      "This sidecar resolves imported plugin manifests and exposes a JSON-RPC tool bridge contract.",
      "Plugin tool calls run only for declared adapter methods; otherwise they return adapter-required receipts.",
      "OpenClaw-compatible hook and memory-slot calls are recorded with bounded payload metadata before plugin-specific executors are promoted.",
    ],
    warnings: warnings.concat(catalog.warnings),
  };
}

function buildCatalog(harnessHome) {
  const registry = tryReadJson(path.join(harnessHome, "state", "harness-registry.json")).value;
  const installs = tryReadJson(path.join(harnessHome, "plugins", "installs.json")).value;
  return buildCatalogFromData(harnessHome, registry, installs);
}

function buildCatalogFromData(harnessHome, registry, installs) {
  const warnings = [];
  const installPlugins = Array.isArray(installs?.plugins) ? installs.plugins : [];
  const installById = new Map(installPlugins.map((plugin) => [String(plugin.pluginId || ""), plugin]));
  const registryPlugins = Array.isArray(registry?.plugins) ? registry.plugins : [];
  const sourceRoots = pluginSourceRoots();
  const plugins = [];
  const tools = [];

  for (const registryPlugin of registryPlugins) {
    const pluginId = String(registryPlugin.id || "");
    if (!pluginId) {
      continue;
    }
    const installRecord = installById.get(pluginId) || null;
    const enabled = registryPlugin.enabled !== false;
    const resolution = resolvePluginManifest(harnessHome, registry, installRecord, pluginId, sourceRoots);
    const manifest = resolution.path ? tryReadJson(resolution.path).value : null;
    const pluginTools = enabled && manifest ? collectManifestTools(pluginId, manifest) : [];
    tools.push(...pluginTools);
    if (registryPlugin.sidecarRequired === true && !resolution.path) {
      warnings.push(`sidecar-required plugin ${pluginId} has no resolved manifest`);
    }
    plugins.push({
      id: pluginId,
      enabled,
      sidecarRequired: registryPlugin.sidecarRequired === true,
      memoryRelated: registryPlugin.memoryRelated === true,
      channelRelated: registryPlugin.channelRelated === true,
      installPresent: Boolean(installRecord),
      origin: installRecord?.origin || null,
      packageName: installRecord?.packageName || null,
      packageVersion: installRecord?.packageVersion || null,
      manifestPath: resolution.path,
      manifestResolved: Boolean(resolution.path),
      manifestCandidates: resolution.candidates,
      tools: pluginTools.map((tool) => tool.name),
    });
  }

  const sidecarRequired = plugins.filter((plugin) => plugin.sidecarRequired);
  const unresolvedSidecarRequired = sidecarRequired.filter((plugin) => !plugin.manifestResolved);
  const status =
    !registry || !installs
      ? "blocked"
      : unresolvedSidecarRequired.length === 0
        ? "ready"
        : "partial";

  return {
    schema: CATALOG_SCHEMA,
    status,
    harnessHome,
    sourceRoots,
    summary: {
      plugins: plugins.length,
      sidecarRequired: sidecarRequired.length,
      resolvedManifests: plugins.filter((plugin) => plugin.manifestResolved).length,
      unresolvedSidecarRequired: unresolvedSidecarRequired.length,
      tools: tools.length,
    },
    plugins,
    tools,
    unresolvedSidecarRequired: unresolvedSidecarRequired.map((plugin) => plugin.id),
    warnings,
  };
}

function pluginSourceRoots() {
  const raw = process.env.AGENT_HARNESS_PLUGIN_SOURCE_ROOTS || "";
  return raw
    .split(path.delimiter)
    .flatMap((entry) => entry.split("|"))
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function resolvePluginManifest(harnessHome, registry, installRecord, pluginId, sourceRoots) {
  const candidates = [];
  const add = (candidate) => {
    if (candidate && !candidates.includes(candidate)) {
      candidates.push(candidate);
    }
  };

  add(path.join(harnessHome, "plugins", "manifests", pluginId, "openclaw.plugin.json"));
  if (installRecord?.manifestPath) {
    add(resolveSourcePath(registry, installRecord.manifestPath));
  }
  for (const root of sourceRoots) {
    add(path.join(root, pluginId, "openclaw.plugin.json"));
    add(path.join(root, "extensions", pluginId, "openclaw.plugin.json"));
    add(path.join(root, "root", ".openclaw", "workspace", pluginId, "openclaw.plugin.json"));
    add(path.join(root, "root", ".openclaw", "workspace", ".openclaw", "extensions", pluginId, "openclaw.plugin.json"));
    add(path.join(root, "root", ".openclaw", "workspace", "openclaw-mem", "extensions", pluginId, "openclaw.plugin.json"));
    add(path.join(root, "root", ".openclaw", "workspace", "openclaw-mem-prod", "extensions", pluginId, "openclaw.plugin.json"));
  }

  const resolved = candidates.find((candidate) => fs.existsSync(candidate));
  return { path: resolved || null, candidates };
}

function resolveSourcePath(registry, rawPath) {
  if (!rawPath) {
    return null;
  }
  if (!rawPath.startsWith("/")) {
    return rawPath;
  }
  const sourceHome = registry?.sourceHome || null;
  const sourceWorkspace = registry?.sourceWorkspace || null;
  const workspacePrefix = "/root/.openclaw/workspace/";
  if (rawPath.startsWith(workspacePrefix) && sourceWorkspace) {
    return joinUnixRelative(sourceWorkspace, rawPath.slice(workspacePrefix.length));
  }
  const homePrefix = "/root/.openclaw/";
  if (rawPath.startsWith(homePrefix) && sourceHome) {
    return joinUnixRelative(sourceHome, rawPath.slice(homePrefix.length));
  }
  return rawPath;
}

function joinUnixRelative(root, relative) {
  return relative.split("/").filter(Boolean).reduce((current, part) => path.join(current, part), root);
}

function collectManifestTools(pluginId, manifest) {
  const tools = [];
  addToolContracts(tools, pluginId, "contracts.tools", manifest?.contracts?.tools);
  addToolContracts(tools, pluginId, "tools", manifest?.tools);
  addToolContracts(tools, pluginId, "commands", manifest?.commands);
  return tools;
}

function addToolContracts(target, pluginId, contractPath, value) {
  if (!value) {
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const tool = normalizeToolContract(pluginId, contractPath, item);
      if (tool) {
        target.push(tool);
      }
    }
    return;
  }
  if (typeof value === "object") {
    for (const [name, contract] of Object.entries(value)) {
      const tool = normalizeToolContract(pluginId, contractPath, { name, ...objectValue(contract) });
      if (tool) {
        target.push(tool);
      }
    }
  }
}

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function normalizeToolContract(pluginId, contractPath, item) {
  const name = typeof item === "string" ? item : String(item?.name || item?.id || "");
  if (!name) {
    return null;
  }
  return {
    id: `${pluginId}.${name}`,
    pluginId,
    name,
    contractPath,
    description: typeof item === "object" && item?.description ? String(item.description) : null,
    inputSchema: typeof item === "object" && item?.inputSchema ? item.inputSchema : null,
    executorStatus: "adapter-required",
  };
}

function writeProbeReceipt(harnessHome, probe) {
  const dir = path.join(harnessHome, "state", "plugin-sidecar");
  fs.mkdirSync(dir, { recursive: true });
  const probeFile = path.join(dir, "probe.json");
  const receiptsFile = path.join(dir, "probe-receipts.jsonl");
  fs.writeFileSync(probeFile, `${JSON.stringify(probe, null, 2)}\n`);
  const receipt = {
    schema: PROBE_RECEIPT_SCHEMA,
    status: probe.status,
    probeFile,
    sidecarRequired: probe.summary.sidecarRequired,
    resolvedManifests: probe.summary.resolvedManifests,
    unresolvedSidecarRequired: probe.summary.unresolvedSidecarRequired,
    tools: probe.summary.tools,
    reason:
      probe.status === "contract-ready"
        ? "plugin sidecar probe loaded harness registry and plugin catalog"
        : "plugin sidecar probe could not load harness registry",
  };
  fs.appendFileSync(receiptsFile, `${JSON.stringify(receipt)}\n`);
  return { probeFile, receiptsFile, receipt };
}

function writeCatalogAndExecutionReceipt(harnessHome, method, catalog, extra = {}) {
  const dir = path.join(harnessHome, "state", "plugin-sidecar");
  fs.mkdirSync(dir, { recursive: true });
  const catalogFile = path.join(dir, "catalog.json");
  const receiptsFile = path.join(dir, "execution-receipts.jsonl");
  fs.writeFileSync(catalogFile, `${JSON.stringify(catalog, null, 2)}\n`);
  const receipt = {
    schema: EXECUTION_RECEIPT_SCHEMA,
    status: catalog.status,
    method,
    catalogFile,
    sidecarRequired: catalog.summary.sidecarRequired,
    resolvedManifests: catalog.summary.resolvedManifests,
    unresolvedSidecarRequired: catalog.summary.unresolvedSidecarRequired,
    tools: catalog.summary.tools,
    reason:
      catalog.status === "ready"
        ? "plugin sidecar manifest catalog is ready"
        : catalog.status === "partial"
          ? "plugin sidecar manifest catalog has unresolved sidecar-required plugins"
          : "plugin sidecar manifest catalog is blocked",
    ...extra,
  };
  fs.appendFileSync(receiptsFile, `${JSON.stringify(receipt)}\n`);
  return receipt;
}

async function runStdio(harnessHome) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false,
  });
  for await (const line of rl) {
    if (!line.trim()) {
      continue;
    }
    let request;
    try {
      request = JSON.parse(line);
      const result = handleRequest(harnessHome, request);
      process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id: request.id ?? null, result })}\n`);
    } catch (error) {
      process.stdout.write(
        `${JSON.stringify({
          jsonrpc: "2.0",
          id: request?.id ?? null,
          error: { code: -32000, message: error.message },
        })}\n`,
      );
    }
  }
}

function handleRequest(harnessHome, request) {
  const method = request.method;
  if (method === "sidecar.status") {
    return buildProbe(harnessHome);
  }
  if (method === "plugins.list") {
    return buildCatalog(harnessHome).plugins;
  }
  if (method === "tools.list") {
    return buildCatalog(harnessHome).tools;
  }
  if (method === "tools.probe") {
    const catalog = buildCatalog(harnessHome);
    const receipt = writeCatalogAndExecutionReceipt(harnessHome, method, catalog);
    return {
      schema: catalog.schema,
      status: catalog.status,
      summary: catalog.summary,
      unresolvedSidecarRequired: catalog.unresolvedSidecarRequired,
      warnings: catalog.warnings,
      catalogFile: receipt.catalogFile,
      receipt,
    };
  }
  if (method === "tools.call") {
    return handleToolCall(harnessHome, request.params || {});
  }
  if (method === "hooks.invoke") {
    return handleHookInvoke(harnessHome, request.params || {});
  }
  if (method === "memory.slot") {
    return handleMemorySlot(harnessHome, request.params || {});
  }
  if (method === "openclaw_mem.status") {
    return handleOpenClawMemStatus(harnessHome, request.params || {});
  }
  if (method === "openclaw_mem.recall") {
    return handleOpenClawMemRecall(harnessHome, request.params || {});
  }
  if (method === "openclaw_mem.propose") {
    return handleOpenClawMemPropose(harnessHome, request.params || {});
  }
  if (method === "openclaw_mem.store") {
    return handleOpenClawMemStore(harnessHome, request.params || {});
  }
  throw new Error(`unknown method: ${method}`);
}

function handleToolCall(harnessHome, params) {
  const toolId = String(params.toolId || params.name || "");
  if (!toolId) {
    throw new Error("tools.call requires params.toolId");
  }
  const catalog = buildCatalog(harnessHome);
  const tool = catalog.tools.find((entry) => entry.id === toolId || entry.name === toolId);
  if (!tool) {
    const receipt = writeCatalogAndExecutionReceipt(harnessHome, "tools.call", catalog, {
      toolId,
      toolStatus: "not-found",
    });
    throw new Error(`tool not found in plugin sidecar catalog: ${toolId}; receipt=${receipt.catalogFile}`);
  }
  const receipt = writeCatalogAndExecutionReceipt(harnessHome, "tools.call", catalog, {
    toolId: tool.id,
    toolStatus: "adapter-required",
  });
  return {
    status: "adapter-required",
    tool,
    receipt,
    message: "Plugin-specific tool execution adapters are not implemented yet.",
  };
}

function handleHookInvoke(harnessHome, params) {
  const hook = String(params.hook || params.name || "");
  if (!hook) {
    throw new Error("hooks.invoke requires params.hook");
  }
  const pluginId = params.pluginId ? String(params.pluginId) : null;
  const payload = params.payload && typeof params.payload === "object" ? params.payload : {};
  const receipt = writeHookReceipt(harnessHome, {
    hook,
    pluginId,
    payload,
    status: "recorded",
    reason: "OpenClaw-compatible hook invocation recorded for adapter dispatch",
  });
  return {
    status: "recorded",
    hook,
    pluginId,
    receipt,
    message: "Hook invocation was recorded; plugin-specific executors can consume this receipt.",
  };
}

function handleMemorySlot(harnessHome, params) {
  const operation = String(params.operation || params.name || "");
  if (!operation) {
    throw new Error("memory.slot requires params.operation");
  }
  const slot = String(params.slot || "memory");
  const pluginId = params.pluginId ? String(params.pluginId) : null;
  const payload = params.payload && typeof params.payload === "object" ? params.payload : {};
  const receipt = writeMemorySlotReceipt(harnessHome, {
    slot,
    operation,
    pluginId,
    payload,
    status: "recorded",
    reason: "OpenClaw-compatible memory-slot operation recorded for adapter dispatch",
  });
  return {
    status: "recorded",
    slot,
    operation,
    pluginId,
    receipt,
    message: "Memory-slot operation was recorded; memory adapters can consume this receipt.",
  };
}

function writeHookReceipt(harnessHome, entry) {
  const dir = path.join(harnessHome, "state", "plugin-sidecar");
  fs.mkdirSync(dir, { recursive: true });
  const receiptsFile = path.join(dir, "hook-receipts.jsonl");
  const receipt = {
    schema: HOOK_RECEIPT_SCHEMA,
    status: entry.status,
    hook: entry.hook,
    pluginId: entry.pluginId,
    payloadKeys: Object.keys(entry.payload || {}).sort(),
    payloadBytes: Buffer.byteLength(JSON.stringify(entry.payload || {}), "utf8"),
    reason: entry.reason,
    atMs: Date.now(),
  };
  fs.appendFileSync(receiptsFile, `${JSON.stringify(receipt)}\n`);
  return { receiptsFile, ...receipt };
}

function writeMemorySlotReceipt(harnessHome, entry) {
  const dir = path.join(harnessHome, "state", "plugin-sidecar");
  fs.mkdirSync(dir, { recursive: true });
  const receiptsFile = path.join(dir, "memory-slot-receipts.jsonl");
  const receipt = {
    schema: MEMORY_SLOT_RECEIPT_SCHEMA,
    status: entry.status,
    slot: entry.slot,
    operation: entry.operation,
    pluginId: entry.pluginId,
    payloadKeys: Object.keys(entry.payload || {}).sort(),
    payloadBytes: Buffer.byteLength(JSON.stringify(entry.payload || {}), "utf8"),
    reason: entry.reason,
    atMs: Date.now(),
  };
  fs.appendFileSync(receiptsFile, `${JSON.stringify(receipt)}\n`);
  return { receiptsFile, ...receipt };
}

function handleOpenClawMemStatus(harnessHome, params) {
  const agentId = stringParam(params.agentId || params.agent || null);
  const qdrantEdgeDir = path.join(harnessHome, "memory", "qdrant-edge");
  const sqliteDatabase = path.join(harnessHome, "memory", "openclaw-mem.sqlite");
  const observationsFile = path.join(harnessHome, "memory", "openclaw-mem-observations.jsonl");
  const episodesFile = path.join(harnessHome, "memory", "openclaw-mem-episodes.jsonl");
  const storeFile = openClawMemStoreFile(harnessHome, agentId);
  const endpoint = stringParam(process.env.AGENT_HARNESS_OPENCLAW_MEM_SERVICE_URL || null);
  const hasReadable =
    fs.existsSync(sqliteDatabase) ||
    fs.existsSync(observationsFile) ||
    fs.existsSync(episodesFile) ||
    fs.existsSync(storeFile);
  const qdrantPresent = fs.existsSync(qdrantEdgeDir);
  const warnings = [];
  if (endpoint) {
    warnings.push(
      "Live openclaw-mem service endpoint is configured, but no remote wire contract is available in the imported artifacts; sidecar is using local snapshot/writeback files."
    );
  } else {
    warnings.push("No live openclaw-mem service endpoint configured; sidecar is using local snapshot/writeback files.");
  }
  if (qdrantPresent) {
    warnings.push("Qdrant edge is present as an imported snapshot; this sidecar does not raw-read it as a live Qdrant service.");
  }
  const status = hasReadable ? "ready" : qdrantPresent ? "degraded" : "blocked";
  const result = {
    schema: "agent-harness.openclaw-mem-sidecar-status.v1",
    status,
    harnessHome,
    agentId,
    serviceMode: "snapshot-adapter",
    serviceEndpoint: endpoint,
    qdrantEdgeDir: qdrantPresent ? qdrantEdgeDir : null,
    qdrantEdgeMode: qdrantPresent ? "preserved-snapshot" : "missing",
    sqliteDatabase: fs.existsSync(sqliteDatabase) ? sqliteDatabase : null,
    observationsFile: fs.existsSync(observationsFile) ? observationsFile : null,
    episodesFile: fs.existsSync(episodesFile) ? episodesFile : null,
    storeFile,
    capabilities: ["status", "recall", "propose", "store-approved"],
    warnings,
  };
  const receipt = writeOpenClawMemSidecarReceipt(harnessHome, "openclaw_mem.status", result, agentId);
  return { ...result, receipt };
}

function handleOpenClawMemRecall(harnessHome, params) {
  const agentId = stringParam(params.agentId || params.agent || null);
  const query = stringParam(params.query || "");
  const limit = positiveInt(params.limit, 5);
  if (!query) {
    throw new Error("openclaw_mem.recall requires params.query");
  }
  const hits = [];
  for (const file of openClawMemSearchFiles(harnessHome, agentId)) {
    if (!fs.existsSync(file)) {
      continue;
    }
    const lines = fs.readFileSync(file, "utf8").split(/\r?\n/).filter(Boolean);
    for (const [index, line] of lines.entries()) {
      const text = jsonLineText(line);
      const score = lexicalScore(query, text);
      if (score > 0) {
        hits.push({
          lane: file.includes("openclaw-mem-service-store") ? "service-writeback" : "snapshot-jsonl",
          id: `${file}:${index + 1}`,
          score,
          title: path.basename(file),
          text: text.slice(0, 320),
          source: file,
        });
      }
    }
  }
  hits.sort((a, b) => b.score - a.score);
  const result = {
    schema: "agent-harness.openclaw-mem-sidecar-recall.v1",
    status: hits.length ? "ready" : "no-hits",
    harnessHome,
    agentId,
    queryLength: query.length,
    hits: hits.slice(0, limit),
    hitCount: Math.min(hits.length, limit),
    backend: "sidecar-jsonl-snapshot",
    qdrantEdgeMode: fs.existsSync(path.join(harnessHome, "memory", "qdrant-edge"))
      ? "preserved-snapshot"
      : "missing",
  };
  const receipt = writeOpenClawMemSidecarReceipt(harnessHome, "openclaw_mem.recall", result, agentId);
  return { ...result, receipt };
}

function handleOpenClawMemPropose(harnessHome, params) {
  const agentId = stringParam(params.agentId || params.agent || null);
  const sessionKey = stringParam(params.sessionKey || params.session || null);
  const text = redactText(stringParam(params.text || ""));
  if (!text) {
    throw new Error("openclaw_mem.propose requires params.text");
  }
  const proposalId = stableId("proposal", agentId, sessionKey, text);
  const proposal = {
    schema: "openclaw-mem.service-proposal.v1",
    proposalId,
    status: "pending-review",
    agentId,
    sessionKey,
    text: text.slice(0, 1200),
    payload: redactJson(params.payload || {}),
    createdAtMs: Date.now(),
  };
  const proposalFile = openClawMemProposalFile(harnessHome, agentId);
  appendJsonLine(proposalFile, proposal);
  const result = {
    schema: "agent-harness.openclaw-mem-sidecar-proposal.v1",
    status: "pending-review",
    harnessHome,
    agentId,
    sessionKey,
    proposalId,
    proposalFile,
  };
  const receipt = writeOpenClawMemSidecarReceipt(harnessHome, "openclaw_mem.propose", result, agentId);
  return { ...result, receipt };
}

function handleOpenClawMemStore(harnessHome, params) {
  const agentId = stringParam(params.agentId || params.agent || null);
  const sessionKey = stringParam(params.sessionKey || params.session || null);
  const approved = params.approved === true;
  const text = redactText(stringParam(params.text || ""));
  if (!text) {
    throw new Error("openclaw_mem.store requires params.text");
  }
  const storeFile = openClawMemStoreFile(harnessHome, agentId);
  const storeId = approved ? stableId("store", agentId, sessionKey, text) : null;
  if (approved) {
    appendJsonLine(storeFile, {
      schema: "openclaw-mem.service-store.v1",
      storeId,
      agentId,
      sessionKey,
      text: text.slice(0, 1200),
      payload: redactJson(params.payload || {}),
      storedAtMs: Date.now(),
      refs: { source: "agent-plugin-sidecar-openclaw-mem" },
    });
  }
  const result = {
    schema: "agent-harness.openclaw-mem-sidecar-store.v1",
    status: approved ? "stored" : "review-required",
    harnessHome,
    agentId,
    sessionKey,
    storeId,
    storeFile,
    reason: approved
      ? "approved memory stored in sidecar writeback"
      : "store blocked because params.approved was not true",
  };
  const receipt = writeOpenClawMemSidecarReceipt(harnessHome, "openclaw_mem.store", result, agentId);
  return { ...result, receipt };
}

function openClawMemSearchFiles(harnessHome, agentId) {
  return [
    path.join(harnessHome, "memory", "openclaw-mem-observations.jsonl"),
    path.join(harnessHome, "memory", "openclaw-mem-episodes.jsonl"),
    openClawMemStoreFile(harnessHome, agentId),
  ];
}

function openClawMemProposalFile(harnessHome, agentId) {
  return path.join(openClawMemStateDir(harnessHome, agentId), "openclaw-mem-service-proposals.jsonl");
}

function openClawMemStoreFile(harnessHome, agentId) {
  if (agentId) {
    return path.join(harnessHome, "agents", normalizePathPart(agentId), "memory", "openclaw-mem-service-store.jsonl");
  }
  return path.join(harnessHome, "memory", "openclaw-mem-service-store.jsonl");
}

function openClawMemStateDir(harnessHome, agentId) {
  if (agentId) {
    return path.join(harnessHome, "state", "agents", normalizePathPart(agentId), "memory");
  }
  return path.join(harnessHome, "state", "memory");
}

function writeOpenClawMemSidecarReceipt(harnessHome, method, result, agentId) {
  const dir = openClawMemStateDir(harnessHome, agentId);
  fs.mkdirSync(dir, { recursive: true });
  const receiptsFile = path.join(dir, "openclaw-mem-sidecar-receipts.jsonl");
  const receipt = {
    schema: OPENCLAW_MEM_SERVICE_RECEIPT_SCHEMA,
    method,
    status: result.status,
    agentId,
    atMs: Date.now(),
    reason: result.reason || `${method} completed`,
  };
  fs.appendFileSync(receiptsFile, `${JSON.stringify(receipt)}\n`);
  return { receiptsFile, ...receipt };
}

function appendJsonLine(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.appendFileSync(file, `${JSON.stringify(value)}\n`);
}

function jsonLineText(line) {
  try {
    const value = JSON.parse(line);
    return String(value.text || value.summary || value.content || value.payload?.text || line);
  } catch {
    return line;
  }
}

function lexicalScore(query, text) {
  const lower = text.toLowerCase();
  return query
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .filter((term) => lower.includes(term)).length;
}

function positiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function stringParam(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value).trim();
  return text ? text : null;
}

function normalizePathPart(value) {
  return String(value)
    .split("")
    .map((ch) => (/^[A-Za-z0-9_.-]$/.test(ch) ? ch.toLowerCase() : `_u${ch.codePointAt(0).toString(16)}_`))
    .join("") || "unknown";
}

function stableId(kind, agentId, sessionKey, text) {
  let hash = 0xcbf29ce484222325n;
  const input = `${kind}|${agentId || "global"}|${sessionKey || "unknown"}|${text}`;
  for (const ch of Buffer.from(input, "utf8")) {
    hash ^= BigInt(ch);
    hash = BigInt.asUintN(64, hash * 0x100000001b3n);
  }
  return `sidecar:${hash.toString(16).padStart(16, "0")}`;
}

function redactJson(value) {
  if (typeof value === "string") {
    return redactText(value);
  }
  if (Array.isArray(value)) {
    return value.map(redactJson);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => {
        const lower = key.toLowerCase();
        if (lower.includes("key") || lower.includes("token") || lower.includes("secret") || lower.includes("password")) {
          return [key, "[redacted]"];
        }
        return [key, redactJson(child)];
      }),
    );
  }
  return value;
}

function redactText(text) {
  return String(text)
    .split(/\s+/)
    .map((token) =>
      token.startsWith("sk-") || token.toLowerCase().includes("token=") || token.toLowerCase().includes("password=")
        ? "[redacted]"
        : token,
    )
    .join(" ");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.stdio) {
    await runStdio(args.harnessHome);
    return;
  }

  const probe = buildProbe(args.harnessHome);
  if (args.writeReceipt) {
    writeProbeReceipt(args.harnessHome, probe);
  }
  if (args.probe || !args.stdio) {
    process.stdout.write(`${JSON.stringify(probe, null, 2)}\n`);
  }
  if (probe.status !== "contract-ready") {
    process.exitCode = 2;
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(2);
});
