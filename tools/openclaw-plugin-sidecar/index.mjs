#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import readline from "node:readline";

const PROBE_SCHEMA = "openclaw-harness.plugin-sidecar-probe.v1";
const PROBE_RECEIPT_SCHEMA = "openclaw-harness.plugin-sidecar-probe-receipt.v1";
const CATALOG_SCHEMA = "openclaw-harness.plugin-sidecar-catalog.v1";
const EXECUTION_RECEIPT_SCHEMA = "openclaw-harness.plugin-sidecar-execution-receipt.v1";

function parseArgs(argv) {
  const args = {
    harnessHome: process.env.OPENCLAW_HARNESS_HOME || ".openclaw-harness",
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
    capabilities: ["sidecar.status", "plugins.list", "tools.list", "tools.probe", "tools.call"],
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
      "Plugin tool calls are refused until a plugin-specific executor adapter is implemented.",
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
  const raw = process.env.OPENCLAW_PLUGIN_SOURCE_ROOTS || "";
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
    add(resolveOpenClawPath(registry, installRecord.manifestPath));
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

function resolveOpenClawPath(registry, rawPath) {
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
