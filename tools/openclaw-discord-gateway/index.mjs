#!/usr/bin/env node
import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";

const PROBE_SCHEMA = "openclaw-harness.discord-gateway-probe.v1";
const RECEIPT_SCHEMA = "openclaw-harness.discord-gateway-probe-receipt.v1";
const DISCORD_GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";
const DISCORD_API_BASE = "https://discord.com/api/v10";
const DEFAULT_DISCORD_INTENTS = (1 << 0) | (1 << 9) | (1 << 12);

function parseArgs(argv) {
  const args = {
    harnessHome: process.env.OPENCLAW_HARNESS_HOME || ".openclaw-harness",
    openclawHome: process.env.OPENCLAW_HOME || ".openclaw",
    workspace: null,
    runtimeWorkspace: null,
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
    } else if (flag === "--runtime-workspace") {
      i += 1;
      args.runtimeWorkspace = requiredValue(argv, i, flag);
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
    runtimeWorkspace: args.runtimeWorkspace,
    harnessCli: args.harnessCli,
    gatewayUrl: args.gatewayUrl,
    intents: discordIntents(),
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
  let dispatchLogCount = 0;
  const intents = discordIntents();
  const ws = new WebSocket(args.gatewayUrl);

  await new Promise((resolve, reject) => {
    const requestStopIfNeeded = () => {
      if (args.stopFile && fs.existsSync(args.stopFile)) {
        writeGatewayLog(args, "stop-file", { stopFile: args.stopFile });
        writeLoopHeartbeat(args, "stopped", "stop file requested");
        ws.close(1000, "stop file requested");
      }
    };
    ws.addEventListener("open", () => {
      writeGatewayLog(args, "open", { gatewayUrl: args.gatewayUrl });
      writeLoopHeartbeat(args, "connected", "Discord gateway WebSocket opened");
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
      writeLoopHeartbeat(args, "closed", `Discord gateway WebSocket closed code=${event.code}`);
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
        ws.send(JSON.stringify({ op: 2, d: identifyPayload(token, intents) }));
        writeGatewayLog(args, "identify", { intents });
        return;
      }
      if (payload.op === 11) {
        writeGatewayLog(args, "heartbeat-ack", { sequence });
        writeLoopHeartbeat(args, "heartbeat", `Discord heartbeat ack sequence=${sequence}`);
        return;
      }
      if (payload.t === "MESSAGE_CREATE") {
        const result = runHarnessForEvent(args, payload);
        handledMessages += 1;
        writeGatewayLog(args, "message-create", {
          messageId: payload.d?.id,
          channelId: payload.d?.channel_id,
          guildId: payload.d?.guild_id ?? null,
          contentLength: typeof payload.d?.content === "string" ? payload.d.content.length : null,
          status: result.status,
        });
        writeLoopHeartbeat(args, "message-create", `message handled status=${result.status}`);
        if (args.maxMessages > 0 && handledMessages >= args.maxMessages) {
          ws.close(1000, "max messages handled");
        }
      } else if (payload.t === "INTERACTION_CREATE") {
        const result = await handleInteractionCreate(args, payload);
        handledMessages += result.routed ? 1 : 0;
        writeGatewayLog(args, "interaction-create", {
          interactionId: payload.d?.id,
          interactionType: payload.d?.type ?? null,
          name: payload.d?.data?.name ?? null,
          channelId: payload.d?.channel_id ?? null,
          guildId: payload.d?.guild_id ?? null,
          userId: interactionUserId(payload.d),
          ackStatus: result.ackStatus,
          routed: result.routed,
          status: result.status,
          reason: result.reason,
        });
        writeLoopHeartbeat(args, "interaction-create", `interaction handled status=${result.status}`);
        if (args.maxMessages > 0 && handledMessages >= args.maxMessages) {
          ws.close(1000, "max messages handled");
        }
      } else if (payload.t) {
        dispatchLogCount += 1;
        if (payload.t === "READY") {
          writeGatewayLog(args, "ready", {
            sessionId: payload.d?.session_id,
            userId: payload.d?.user?.id,
            username: payload.d?.user?.username,
          });
          writeLoopHeartbeat(args, "ready", `Discord gateway ready username=${payload.d?.user?.username ?? "-"}`);
        } else if (dispatchLogCount <= 20) {
          writeGatewayLog(args, "dispatch", { type: payload.t, sequence });
        }
      }
    });
  });
}

function identifyPayload(token, intents) {
  return {
    token,
    intents,
    properties: {
      os: process.platform,
      browser: "openclaw-harness",
      device: "openclaw-harness",
    },
  };
}

async function handleInteractionCreate(args, payload) {
  const interaction = payload.d ?? {};
  const content = interactionToMessageText(interaction);
  const userId = interactionUserId(interaction);
  const channelId = interaction.channel_id;
  if (!content || !userId || !channelId) {
    const ackStatus = await acknowledgeInteraction(
      interaction,
      "This Discord interaction type is not supported by the OpenClaw harness yet.",
    );
    return {
      ackStatus,
      routed: false,
      status: 0,
      reason: "unsupported or incomplete interaction payload",
    };
  }

  const ackStatus = await acknowledgeInteraction(
    interaction,
    "Routing this through OpenClaw. Reply will appear in this channel.",
  );
  const syntheticPayload = {
    t: "MESSAGE_CREATE",
    s: payload.s,
    d: {
      id: `interaction-${interaction.id}`,
      channel_id: channelId,
      guild_id: interaction.guild_id ?? null,
      content,
      author: {
        id: userId,
        bot: false,
        username:
          interaction.user?.username ??
          interaction.member?.user?.username ??
          "discord-interaction-user",
      },
    },
  };
  const result = runHarnessForEvent(args, syntheticPayload);
  return {
    ackStatus,
    routed: true,
    status: result.status,
    reason: "Discord interaction normalized into channel-run-once",
  };
}

async function acknowledgeInteraction(interaction, content) {
  if (!interaction?.id || !interaction?.token || typeof fetch !== "function") {
    return "skipped";
  }
  try {
    const response = await fetch(
      `${DISCORD_API_BASE}/interactions/${interaction.id}/${interaction.token}/callback`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          type: 4,
          data: { content },
        }),
      },
    );
    return response.ok ? "sent" : `failed-${response.status}`;
  } catch {
    return "failed-network";
  }
}

function interactionUserId(interaction) {
  return interaction?.user?.id ?? interaction?.member?.user?.id ?? null;
}

function interactionToMessageText(interaction) {
  if (interaction?.type !== 2 || !interaction?.data?.name) {
    return null;
  }
  const args = interactionOptionsToText(interaction.data.options ?? []);
  return args ? `/${interaction.data.name} ${args}` : `/${interaction.data.name}`;
}

function interactionOptionsToText(options) {
  const parts = [];
  for (const option of options) {
    if (Array.isArray(option.options)) {
      const nested = interactionOptionsToText(option.options);
      if (nested) {
        parts.push(option.name, nested);
      } else if (option.name) {
        parts.push(option.name);
      }
      continue;
    }
    if (option.value !== undefined && option.value !== null) {
      parts.push(String(option.value));
    } else if (option.name) {
      parts.push(option.name);
    }
  }
  return parts.join(" ").trim();
}

function discordIntents() {
  const raw = process.env.DISCORD_GATEWAY_INTENTS || process.env.DISCORD_INTENTS;
  if (!raw) {
    return DEFAULT_DISCORD_INTENTS;
  }
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) ? value : DEFAULT_DISCORD_INTENTS;
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
  if (args.runtimeWorkspace) {
    cliArgs.push("--runtime-workspace", args.runtimeWorkspace);
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

function writeLoopHeartbeat(args, status, detail) {
  const dir = path.join(args.harnessHome, "state", "supervisor", "loop-heartbeats");
  fs.mkdirSync(dir, { recursive: true });
  const heartbeat = {
    schema: "openclaw-harness.loop-heartbeat.v1",
    name: "discord-gateway-loop",
    status,
    iteration: null,
    detail,
    atMs: Date.now(),
    processId: process.pid,
  };
  fs.writeFileSync(path.join(dir, "discord-gateway-loop.json"), `${JSON.stringify(heartbeat, null, 2)}\n`);
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
