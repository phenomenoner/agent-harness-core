"""Bounded code locations for failures; never exception text, locals or host paths."""
import re


def failure_trace(error, *, _depth=0):
    def name(value):
        if type(value) is str and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,127}", value):
            return value
        return "unknown"

    frames = []
    traceback = error.__traceback__
    for _ in range(64):
        if traceback is None:
            break
        frame = traceback.tb_frame
        module = frame.f_globals.get("__name__", "")
        if type(module) is str and module.startswith("ahc."):
            frames.append({"module": name(module), "function": name(frame.f_code.co_name),
                           "line": traceback.tb_lineno})
        traceback = traceback.tb_next
    result = {"exception_type": name(type(error).__name__), "frames": frames[-8:]}
    # Explicit vocabulary only: never copy an arbitrary exception attribute or
    # infer a remote effect from a diagnostic phase/category.
    phase = getattr(error, "control_phase", None)
    category = getattr(error, "control_error_type", None)
    if type(phase) is str and phase in {
        "helper", "open", "send", "receive", "close", "decode", "unknown"
    }:
        result["control_phase"] = phase
    if type(category) is str and category in {
        "unknown", "closed", "deadline", "write-indeterminate", "eof",
        "invalid-encoding", "malformed-json", "protocol-frame", "reader-io",
        "cleanup-unverified",
    }:
        result["control_error_type"] = category
    # Cleanup can replace the original invocation exception. Preserve both code
    # locations without serializing exception messages or following cycles forever.
    previous = error.__cause__ or error.__context__
    if _depth < 2 and previous is not None and previous is not error:
        result["previous"] = failure_trace(previous, _depth=_depth + 1)
    return result
