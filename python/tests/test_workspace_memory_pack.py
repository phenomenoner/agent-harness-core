"""Pack tool admission, unavailable outcomes and runtime capability propagation."""

from dataclasses import replace

import pytest
from test_foreground_runtime import setup
from test_memory_pack import parse, payload
from test_memory_pack_service import service

from ahc.application.memory_pack_service import MemoryPackUnavailable


@pytest.mark.parametrize("case", ["success", "stale", "scope", "unavailable", "foreign"])
def test_runtime_pack_tool_is_bound_and_does_not_execute_python(tmp_path, case):
    runtime, _, _, _ = setup(tmp_path)
    calls = []

    class Pack:
        def pack(self, session, query, **options):
            calls.append((session, query, options))
            if case == "unavailable":
                raise MemoryPackUnavailable("private-provider-error")
            return parse(payload())

    runtime.memory_pack = service(lambda *a, **kw: pytest.fail("foreign call")) if (
        case == "foreign") else Pack()
    runtime.start()
    try:
        assert "workspace.memory_pack" in runtime.tools.capabilities()
        expected = runtime.workspace.identity
        arguments = {"expected": expected.to_dict(), "query": "orchid",
                     "budget_tokens": 500, "max_items": 2}
        if case == "stale":
            arguments["expected"] = replace(expected, generation=2).to_dict()
        if case == "scope":
            arguments["scope"] = "another-agent"
        result = runtime.tools.invoke("workspace.memory_pack", arguments)
        if case == "success":
            assert result["disposition"] == "observed"
            assert result["content_authority"] == "untrusted-data"
            assert result["result"]["items"][0]["recordRef"] == "obs:1"
            assert result["provenance"]["trust_source"] == "provider-asserted"
            assert calls == [(runtime.session, "orchid", {"budget_tokens": 500, "max_items": 2})]
        elif case == "unavailable":
            assert result["disposition"] == "unavailable" and len(calls) == 1
            assert "private" not in str(result)
        else:
            assert result["disposition"] == "rejected" and not calls
        assert runtime.workspace.port.calls == 0
    finally:
        runtime.close()
    assert "workspace.memory_pack" not in runtime.tools.capabilities()


def test_absent_pack_service_does_not_advertise_recall(tmp_path):
    runtime, _, _, _ = setup(tmp_path)
    runtime.start()
    try:
        assert "workspace.memory_pack" not in runtime.tools.capabilities()
        assert runtime.tools.invoke("workspace.memory_pack", {})["disposition"] == "unsupported"
    finally:
        runtime.close()
