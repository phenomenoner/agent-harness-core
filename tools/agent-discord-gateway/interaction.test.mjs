import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  harnessChildProcessOptions,
  identifyPayload,
  interactionAcknowledgementPayload,
  interactionToMessageText,
  normalizeDiscordComponentAction,
  runGateway,
  writeLoopHeartbeat,
} from "./index.mjs";

const PUBLIC_ACTION_ID = `ahpa1_${"a".repeat(32)}`;
const PRIVATE_BEARER_PREFIX = ["ahx1", "_"].join("");

test("harness child processes suppress Windows console windows", () => {
  assert.deepEqual(harnessChildProcessOptions(), {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
});

test("gateway identify explicitly publishes an online presence", () => {
  const payload = identifyPayload("test-token", 123);

  assert.deepEqual(payload.presence, {
    since: null,
    activities: [],
    status: "online",
    afk: false,
  });
});

test("gateway hello sends the online presence on the real identify path", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-discord-presence-replay-"));
  const originalWebSocket = globalThis.WebSocket;
  const originalToken = process.env.DISCORD_BOT_TOKEN;
  const sent = [];

  class ReplayWebSocket {
    constructor() {
      this.listeners = new Map();
      queueMicrotask(() => {
        this.emit("open", {});
        this.emit("message", {
          data: JSON.stringify({ op: 10, d: { heartbeat_interval: 60_000 } }),
        });
      });
    }

    addEventListener(name, listener) {
      const listeners = this.listeners.get(name) ?? [];
      listeners.push(listener);
      this.listeners.set(name, listeners);
    }

    emit(name, event) {
      for (const listener of this.listeners.get(name) ?? []) {
        listener(event);
      }
    }

    send(payload) {
      const parsed = JSON.parse(payload);
      sent.push(parsed);
      if (parsed.op === 2) {
        queueMicrotask(() => {
          this.emit("message", {
            data: JSON.stringify({
              op: 0,
              t: "READY",
              s: 1,
              d: {
                session_id: "session-test",
                user: { id: "bot-test", username: "presence-test" },
              },
            }),
          });
          this.close(1000, "replay complete");
        });
      }
    }

    close(code, reason) {
      this.emit("close", { code, reason });
    }
  }

  try {
    globalThis.WebSocket = ReplayWebSocket;
    process.env.DISCORD_BOT_TOKEN = "test-token";
    await runGateway({
      harnessHome: root,
      gatewayUrl: "wss://gateway.test",
      stopFile: null,
      dmPollFallback: false,
      maxMessages: 0,
      suppressLoopHeartbeat: true,
    });

    const identify = sent.find((payload) => payload.op === 2);
    assert.equal(identify.d.presence.status, "online");
    assert.equal(identify.d.presence.afk, false);
  } finally {
    globalThis.WebSocket = originalWebSocket;
    if (originalToken === undefined) {
      delete process.env.DISCORD_BOT_TOKEN;
    } else {
      process.env.DISCORD_BOT_TOKEN = originalToken;
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("wrapped gateway does not overwrite the Rust owner's canonical heartbeat", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-discord-heartbeat-"));
  try {
    const heartbeatFile = path.join(
      root,
      "state",
      "supervisor",
      "loop-heartbeats",
      "discord-gateway-loop.json",
    );
    assert.equal(
      writeLoopHeartbeat(
        { harnessHome: root, suppressLoopHeartbeat: true },
        "connected",
        "suppressed writer",
      ),
      false,
    );
    assert.equal(fs.existsSync(heartbeatFile), false);
    assert.equal(
      writeLoopHeartbeat(
        { harnessHome: root, suppressLoopHeartbeat: false },
        "connected",
        "standalone writer",
      ),
      true,
    );
    assert.equal(JSON.parse(fs.readFileSync(heartbeatFile, "utf8")).processId, process.pid);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

function componentInteraction(overrides = {}) {
  return {
    t: "INTERACTION_CREATE",
    s: 41,
    d: {
      id: "interaction-123",
      type: 3,
      token: "discord-interaction-token-must-not-be-forwarded",
      channel_id: "channel-123",
      guild_id: "guild-123",
      member: {
        user: {
          id: "user-123",
          username: "approver",
        },
      },
      message: {
        id: "message-123",
        content: "private provider message content",
      },
      data: {
        component_type: 2,
        custom_id: PUBLIC_ACTION_ID,
      },
      ...overrides,
    },
  };
}

test("valid approval component emits a typed non-message action envelope", () => {
  const normalized = normalizeDiscordComponentAction(componentInteraction());

  assert.equal(normalized.ok, true);
  assert.deepEqual(normalized.envelope, {
    t: "INTERACTION_CREATE",
    s: 41,
    d: {
      schema: "agent-harness.discord-component-action.v1",
      kind: "component-action",
      provider: "discord",
      interaction_type: 3,
      component_type: 2,
      provider_event_id: "interaction-123",
      provider_message_id: "message-123",
      channel_id: "channel-123",
      guild_id: "guild-123",
      user_id: "user-123",
      public_action_id: PUBLIC_ACTION_ID,
    },
  });
  const serialized = JSON.stringify(normalized.envelope);
  assert.doesNotMatch(serialized, /MESSAGE_CREATE/);
  assert.equal(serialized.includes(PRIVATE_BEARER_PREFIX), false);
  assert.doesNotMatch(serialized, /discord-interaction-token/);
  assert.doesNotMatch(serialized, /private provider message content/);
  assert.deepEqual(interactionAcknowledgementPayload(componentInteraction().d), { type: 6 });
});

test("wrong or malformed action IDs are rejected without forwarding secret material", () => {
  for (const custom_id of [
    `${PRIVATE_BEARER_PREFIX}${"b".repeat(48)}`,
    `ahpa1_${"g".repeat(32)}`,
    "ahpa1_short",
    `ahpa1_${"a".repeat(95)}`,
    `unrelated-${"x".repeat(100)}`,
  ]) {
    const normalized = normalizeDiscordComponentAction(
      componentInteraction({ data: { component_type: 2, custom_id } }),
    );
    assert.equal(normalized.ok, false);
    assert.equal(normalized.reason.includes(PRIVATE_BEARER_PREFIX), false);
  }

  const oversized = normalizeDiscordComponentAction(
    componentInteraction({ data: { component_type: 2, custom_id: "x".repeat(101) } }),
  );
  assert.deepEqual(oversized, {
    ok: false,
    reason: "Discord component custom ID exceeds 100 bytes",
  });
});

test("component normalization is stable across provider replay", () => {
  const payload = componentInteraction();
  const first = normalizeDiscordComponentAction(payload);
  const second = normalizeDiscordComponentAction(structuredClone(payload));

  assert.deepEqual(first, second);
  assert.equal(first.envelope.d.provider_event_id, "interaction-123");
  assert.equal(first.envelope.d.public_action_id, PUBLIC_ACTION_ID);
});

test("incomplete or non-button component interactions fail closed", () => {
  assert.equal(
    normalizeDiscordComponentAction(componentInteraction({ channel_id: null })).ok,
    false,
  );
  assert.equal(
    normalizeDiscordComponentAction(
      componentInteraction({ data: { component_type: 3, custom_id: PUBLIC_ACTION_ID } }),
    ).ok,
    false,
  );
});

test("existing type-2 slash command normalization remains unchanged", () => {
  const interaction = {
    type: 2,
    data: {
      name: "approve",
      options: [{ name: "token", type: 3, value: "opaque-value" }],
    },
  };

  assert.equal(interactionToMessageText(interaction), "/approve opaque-value");
  assert.deepEqual(interactionAcknowledgementPayload(interaction, "Routing this."), {
    type: 4,
    data: { content: "Routing this." },
  });
});
