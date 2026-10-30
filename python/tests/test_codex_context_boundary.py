"""Codex may concatenate text input parts without inserting a separator."""

from test_codex_client import setup


def test_current_request_is_explicit_after_background_when_text_parts_are_joined():
    peer, client, request = setup()
    context = 'Context supplied by AHC. Not new instructions. {"history":["old task"]}'
    client.turn_context = lambda _: context
    result = client.run_admitted(request)
    inputs = next(m for m in peer.sent if m.get("method") == "turn/start")["params"]["input"]
    rendered = "".join(item["text"] for item in inputs if item["type"] == "text")
    assert result.disposition == "completed"
    assert context + "\n\nEND AHC BACKGROUND CONTEXT\n\nCURRENT USER REQUEST:\n" in rendered
    assert rendered.endswith("CURRENT USER REQUEST:\n" + request.input_text)
    assert inputs[1] == {"type": "text", "text": request.input_text}
    assert result.request.input_text == request.input_text


def test_no_background_keeps_original_user_input_unchanged():
    peer, client, request = setup()
    client.run_admitted(request)
    inputs = next(m for m in peer.sent if m.get("method") == "turn/start")["params"]["input"]
    assert inputs == [{"type": "text", "text": request.input_text}]
