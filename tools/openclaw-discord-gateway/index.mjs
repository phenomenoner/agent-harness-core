#!/usr/bin/env node
import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";

const PROBE_SCHEMA = "openclaw-harness.discord-gateway-probe.v1";
const RECEIPT_SCHEMA = "openclaw-harness.discord-gateway-probe-receipt.v1";
const DISCORD_GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";
const DISCORD_INTENTS = 1 << 12;

function parseArgs(argv) {
  const args = {
    harnessHome: process.env.OPENCLAW_HARNESS_HOME || ".openclaw-harness",
    openclawHome: process.env.OPENCLAW_HOME || ".openclaw",
    workspace: null,
    harnessCli: process.env.OPENCLAW_HARNESS_CLI || defaultHarnessCli(),
    agent: null,
    codexExe: null,
    gatewayUrl: DISCORD_GATEWAY_URL,
    maxMessages: 0,
    stopFile: null,
    probe: false,
    writeReceipt: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const flag = argv[i];
    if (flag === "--harness-home" || flag === "--target-home") {
      i += 1;
      args.harnessHome = requiredValue(argv, i, flag);
    } else if (flag === "--openclaw-home") {
      i += 1;
      args.openclawHome = requiredValue(argv, i, flag);
    } else if (flag === "--workspace") {
      i += 1;
      args.workspace = requiredValue(argv, i, flag);
    } else if (flag === "--harness-cli") {
      i += 1;
      args.harnessCli = requiredValue(argv, i, flag);
    } else if (flag === "--agent") {
      i += 1;
      args.agent = requiredValue(argv, i, flag);
    } else if (flag === "--codex-exe") {
      i += 1;
      args.codexExe = requiredValue(argv, i, flag);
    } else if (flag === "--gateway-url") {
      i += 1;
      args.gatewayUrl = requiredValue(argv, i, flag);
    } else if (flag === "--max-messages") {
      i += 1;
      args.maxMessages = Number.parseInt(requiredValue(argv, i, flag), 10);
    } else if (flag === "--stop-file") {
      i += 1;
      args.stopFile = requiredValue(argv, i, flag);
    } else if (flag === "--probe") {
      args.probe = true;
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

function defaultHarnessCli() {
  const exe = os.platform() === "win32" ? "openclaw-harness.exe" : "openclaw-harness";
  return path.join("target", "debug", exe);
}

function buildProbe(args) {
  const tokenPresent = Boolean(process.env.DISCORD_BOT_TOKEN);
  const webSocketPresent = typeof globalThis.WebSocket === "function";
  const harnessCliPresent = fs.existsSync(args.harnessCli);
  const status = webSocketPresent && tokenPresent ? "ready" : tokenPresent ? "blocked" : "token-missing";
  const warnings = [];
  if (!webSocketPresent) {
    warnings.push("Node global WebSocket is unavailable; use Node 22+ or provide a gateway transport dependency.");
  }
  if (!tokenPresent) {
    warnings.push("DISCORD_BOT_TOKEN is missing.");
  }
  if (!harnessCliPresent) {
    warnings.push(`harness CLI not found at ${args.harnessCli}; pass --harness-cli after building.`);
  }
  return {
    schema: PROBE_SCHEMA,
    status,
    harnessHome: args.harnessHome,
    openclawHome: args.openclawHome,
    workspace: args.workspace,
    harnessCli: args.harnessCli,
    gatewayUrl: args.gatewayUrl,
    node: process.version,
    webSocketPresent,
    tokenPresent,
    harnessCliPresent,
    capabilities: [
      "discord.gateway.probe",
      "discord.gateway.heartbeat",
      "discord.gateway.identify",
      "discord.gateway.message-create",
      "discord.gateway.event-run-once",
    ],
    warnings,
  };
}

function writeProbeReceipt(args, probe) {
  const dir = path.join(args.harnessHome, "state", "channels");
  fs.mkdirSync(dir, { recursive: true });
  const probeFile = path.join(dir, "discord-gateway-probe.json");
  const receiptsFile = path.join(dir, "discord-gateway-probe-receipts.jsonl");
  fs.writeFileSync(probeFile, `${JSON.stringify(probe, null, 2)}\n`);
  const receipt = {
    schema: RECEIPT_SCHEMA,
    status: probe.status,
    probeFile,
    tokenPresent: probe.tokenPresent,
    webSocketPresent: probe.webSocketPresent,
    harnessCliPresent: probe.harnessCliPresent,
    reason:
      probe.status === "ready"
        ? "Discord gateway probe is ready to connect"
        : "Discord gateway probe is not ready to connect",
  };
  fs.appendFileSync(receiptsFile, `${JSON.stringify(receipt)}\n`);
  return { probeFile, receiptsFile, receipt };
}

async function runGateway(args) {
  const token = process.env.DISCORD_BOT_TOKEN;
  if (!token) {
    throw new Error("DISCORD_BOT_TOKEN is required for Discord gateway loop");
  }
  if (typeof globalThis.WebSocket !== "function") {
    throw new Error("Node global WebSocket is unavailable");
  }

  let sequence = null;
  let heartbeatTimer = null;
  let stopFileTimer = null;
  let handledMessages = 0;
  const ws = new WebSocket(args.gatewayUrl);

  await new Promise((resolve, reject) => {
    const requestStopIfNeeded = () => {
      if (args.stopFile && fs.existsSync(args.stopFile)) {
        writeGatewayLog(args, "stop-file", { stopFile: args.stopFile });
        ws.close(1000, "stop file requested");
      }
    };
    ws.addEventListener("open", () => {
      writeGatewayLog(args, "open", { gatewayUrl: args.gatewayUrl });
      stopFileTimer = setInterval(requestStopIfNeeded, 1000);
      requestStopIfNeeded();
    });
    ws.addEventListener("error", (event) => {
      reject(new Error(`Discord gateway WebSocket error: ${event.message || "unknown error"}`));
    });
    ws.addEventListener("close", (event) => {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
      }
      if (stopFileTimer) {
        clearInterval(stopFileTimer);
      }
      writeGatewayLog(args, "close", { code: event.code, reason: event.reason });
      resolve();
    });
    ws.addEventListener("message", async (event) => {
      const payload = JSON.parse(String(event.data));
      if (typeof payload.s === "number") {
        sequence = payload.s;
      }
      if (payload.op === 10) {
        const interval = payload.d.heartbeat_interval;
        heartbeatTimer = setInterval(() => {
          ws.send(JSON.stringify({ op: 1, d: sequence }));
        }, interval);
        ws.send(JSON.stringify({ op: 2, d: identifyPayload(token) }));
        writeGatewayLog(args, "identify", { intents: DISCORD_INTENTS });
        return;
      }
      if (payload.op === 11) {
        writeGatewayLog(args, "heartbeat-ack", { sequence });
        return;
      }
      if (payload.t === "MESSAGE_CREATE") {
        const result = runHarnessForEvent(args, payload);
        handledMessages += 1;
        writeGatewayLog(args, "message-create", {
          messageId: payload.d?.id,
          channelId: payload.d?.channel_id,
          status: result.status,
        });
        if (args.maxMessages > 0 && handledMessages >= args.maxMessages) {
          ws.close(1000, "max messages handled");
        }
      }
    });
  });
}

function identifyPayload(token) {
  return {
    token,
    intents: DISCORD_INTENTS,
    properties: {
      os: process.platform,
      browser: "openclaw-harness",
      device: "openclaw-harness",
    },
  };
}

function runHarnessForEvent(args, payload) {
  const eventFile = writeTempEventFile(payload);
  const cliArgs = [
    "discord-event-run-once",
    "--harness-home",
    args.harnessHome,
    "--openclaw-home",
    args.openclawHome,
    "--event-file",
    eventFile,
  ];
  if (args.workspace) {
    cliArgs.push("--workspace", args.workspace);
  }
  if (args.agent) {
    cliArgs.push("--agent", args.agent);
  }
  if (args.codexExe) {
    cliArgs.push("--codex-exe", args.codexExe);
  }
  const result = childProcess.spawnSync(args.harnessCli, cliArgs, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  removeTempEventFile(eventFile);
  if (result.stdout) {
    writeGatewayLog(args, "harness-stdout", { bytes: Buffer.byteLength(result.stdout, "utf8") });
  }
  if (result.stderr) {
    writeGatewayLog(args, "harness-stderr", { bytes: Buffer.byteLength(result.stderr, "utf8") });
  }
  return { status: result.status ?? 1 };
}

function writeTempEventFile(payload) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-discord-event-"));
  const file = path.join(dir, "event.json");
  fs.writeFileSync(file, `${JSON.stringify(payload)}\n`);
  return file;
}

function removeTempEventFile(file) {
  try {
    fs.rmSync(path.dirname(file), { recursive: true, force: true });
  } catch {
    // Temp cleanup failure should not hide the harness result.
  }
}

function writeGatewayLog(args, event, payload) {
  const dir = path.join(args.harnessHome, "state", "channels");
  fs.mkdirSync(dir, { recursive: true });
  const logFile = path.join(dir, "discord-gateway-events.jsonl");
  fs.appendFileSync(logFile, `${JSON.stringify({ event, ...payload })}\n`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.probe) {
    const probe = buildProbe(args);
    if (args.writeReceipt) {
      writeProbeReceipt(args, probe);
    }
    process.stdout.write(`${JSON.stringify(probe, null, 2)}\n`);
    if (probe.status === "blocked") {
      process.exitCode = 2;
    }
    return;
  }
  await runGateway(args);
}

main().catch((error) => {
  console.error(error.message);
  process.exit(2);
});
