#!/usr/bin/env node
import readline from "node:readline";

const args = new Set(process.argv.slice(2));
const reply = process.env.OPENCLAW_FAKE_CODEX_REPLY || "OpenClaw fake Codex reply.";
const threadId = process.env.OPENCLAW_FAKE_CODEX_THREAD_ID || "thread-openclaw-fake";
const turnId = process.env.OPENCLAW_FAKE_CODEX_TURN_ID || "turn-openclaw-fake";
const stayOpen = args.has("--stay-open");

let completedTurn = false;

function write(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

function handleMessage(message) {
  if (message?.id === 0) {
    write({ id: 0, result: { ok: true } });
    return;
  }

  if (message?.method === "thread/start") {
    write({
      id: message.id ?? 1,
      result: {
        thread: {
          id: threadId,
        },
      },
    });
    return;
  }

  if (message?.method === "turn/start") {
    write({
      method: "item/agentMessage/delta",
      params: {
        delta: reply,
      },
    });
    write({
      method: "turn/completed",
      params: {
        turn: {
          id: turnId,
          status: "completed",
        },
      },
    });
    completedTurn = true;
    if (!stayOpen) {
      process.stdin.pause();
      setImmediate(() => process.exit(0));
    }
  }
}

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Number.POSITIVE_INFINITY,
});

rl.on("line", (line) => {
  let message;
  try {
    message = JSON.parse(line);
  } catch (error) {
    process.stderr.write(`ignored invalid JSONL input: ${error.message}\n`);
    return;
  }
  handleMessage(message);
});

rl.on("close", () => {
  if (!completedTurn || stayOpen) {
    process.exit(0);
  }
});
