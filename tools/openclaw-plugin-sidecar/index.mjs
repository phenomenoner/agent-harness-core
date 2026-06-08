#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import readline from "node:readline";

const PROBE_SCHEMA = "openclaw-harness.plugin-sidecar-probe.v1";
const RECEIPT_SCHEMA = "openclaw-harness.plugin-sidecar-probe-receipt.v1";

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

function buildProbe(harnessHome) {
  const registryFile = path.join(harnessHome, "state", "harness-registry.json");
  const installsFile = path.join(harnessHome, "plugins", "installs.json");
  const warnings = [];
  let registry = null;
  let installsPresent = false;

  try {
    registry = readJson(registryFile);
  } catch (error) {
    warnings.push(`registry unavailable: ${error.message}`);
  }

  try {
    installsPresent = fs.statSync(installsFile).isFile();
  } catch {
    installsPresent = false;
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
  const status = registry ? "contract-ready" : "blocked";

  return {
    schema: PROBE_SCHEMA,
    status,
    harnessHome,
    registryFile,
    installsFile,
    installsPresent,
    pid: process.pid,
    node: process.version,
    capabilities: ["sidecar.status", "plugins.list", "tools.list"],
    summary: {
      plugins: plugins.length,
      sidecarRequired: sidecarRequired.length,
      notSidecarRequired: notSidecarRequired.length,
    },
    sidecarRequired,
    notSidecarRequired,
    limitations: [
      "This probe validates sidecar process startup and imported plugin metadata only.",
      "OpenClaw plugin hook/tool execution bridge is not implemented by this probe.",
    ],
    warnings,
  };
}

function writeProbeReceipt(harnessHome, probe) {
  const dir = path.join(harnessHome, "state", "plugin-sidecar");
  fs.mkdirSync(dir, { recursive: true });
  const probeFile = path.join(dir, "probe.json");
  const receiptsFile = path.join(dir, "probe-receipts.jsonl");
  fs.writeFileSync(probeFile, `${JSON.stringify(probe, null, 2)}\n`);
  const receipt = {
    schema: RECEIPT_SCHEMA,
    status: probe.status,
    probeFile,
    sidecarRequired: probe.summary.sidecarRequired,
    reason:
      probe.status === "contract-ready"
        ? "plugin sidecar probe loaded harness registry"
        : "plugin sidecar probe could not load harness registry",
  };
  fs.appendFileSync(receiptsFile, `${JSON.stringify(receipt)}\n`);
  return { probeFile, receiptsFile, receipt };
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
    return buildProbe(harnessHome).sidecarRequired;
  }
  if (method === "tools.list") {
    return [];
  }
  throw new Error(`unknown method: ${method}`);
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
