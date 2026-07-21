import assert from "node:assert/strict";
import test from "node:test";

import {
  interactionAcknowledgementPayload,
  interactionToMessageText,
  normalizeDiscordComponentAction,
} from "./index.mjs";

const PUBLIC_ACTION_ID = `ahpa1_${"a".repeat(32)}`;
const PRIVATE_BEARER_PREFIX = ["ahx1", "_"].join("");

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
