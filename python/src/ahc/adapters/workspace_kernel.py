"""Fixed interpreter loop delivered only inside the OS-isolated container.

This is not a Python language sandbox. Generated code has normal Python semantics
inside its container; host authority is never taken from its output or namespace.
"""

KERNEL = r"""import base64, contextlib, hashlib, io, json, sys
namespace = {"__name__": "__workspace__"}
def checkpoint_values(names):
    def snapshot(value, depth=0):
        if depth > 8:
            raise ValueError()
        kind = type(value)
        if value is None or kind in (bool, int, str):
            return value
        if kind is list:
            return [snapshot(item, depth + 1) for item in value]
        if kind is dict and all(type(key) is str for key in value):
            return {key: snapshot(item, depth + 1) for key, item in value.items()}
        raise ValueError()
    # Copy only exact portable primitives; never call user serialization hooks.
    return {name: snapshot(namespace[name]) for name in names}
def artifact_exporter():
    # One immutable snapshot, private to the control loop, not the user namespace.
    content = identifier = digest = None
    offset = 0
    def export(request):
        nonlocal content, identifier, digest, offset
        if "name" in request:
            content = None
            value = namespace[request["name"]]
            if type(value) is str:
                value = value.encode("utf-8")
            elif type(value) is not bytes:
                raise ValueError()
            content, identifier, offset = value, request["id"], 0
            digest = hashlib.sha256(content).hexdigest()
        elif (content is None or request.get("export_id") != identifier
              or type(request.get("offset")) is not int or request["offset"] != offset):
            raise ValueError()
        chunk = content[offset:offset + 32768]
        reply = {"export_id": identifier, "offset": offset, "byte_count": len(content),
                 "content_digest": digest, "data": base64.b64encode(chunk).decode("ascii")}
        offset += len(chunk)
        if offset == len(content):
            content = None
        return reply
    return export
export_artifact_chunk = artifact_exporter()
def batch_facade(identifier):
    def batch(calls, concurrency=1):
        message = json.dumps({"id": identifier, "batch": calls,
                              "concurrency": concurrency}, allow_nan=False)
        if len(message.encode()) + 1 > 65536:
            raise ValueError("batch request too large")
        sys.__stdout__.write(message + "\n")
        sys.__stdout__.flush()
        response = json.loads(sys.stdin.readline(65537))
        if response.get("id") != identifier or "batch_result" not in response:
            raise ValueError("batch response invalid")
        return response["batch_result"]
    return batch
class BoundedOutput(io.StringIO):
    truncated = False
    def write(self, text):
        remaining = 4096 - self.tell()
        if len(text) > remaining:
            self.truncated = True
        if remaining > 0:
            super().write(text[:remaining])
        return len(text)
for line in sys.stdin:
    request = json.loads(line)
    result = {"id": request["id"], "ok": True}
    try:
        if request["operation"] == "execute":
            namespace.pop("ahc_batch", None)
            if request.get("batch_enabled") is True:
                namespace["ahc_batch"] = batch_facade(request["id"])
            output = BoundedOutput()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                try:
                    exec(compile(request["code"], "<workspace>", "exec"), namespace)
                except BaseException as exc:
                    result["ok"] = False
                    result["error"] = "execution-error"
                    if (type(exc) in (SyntaxError, IndentationError, TabError)
                        and exc.filename == "<workspace>"
                        and type(exc.lineno) is int and type(exc.offset) is int
                        and 1 <= exc.lineno <= len(request["code"].splitlines())
                        and 1 <= exc.offset <= len(request["code"].splitlines()[exc.lineno-1])+1):
                        output.write("SyntaxError at submitted line " + str(exc.lineno)
                                     + ", column " + str(exc.offset)
                                     + ". Correct the syntax in a smaller cell; use the latest "
                                       "returned workspace identity.\n")
                    else:
                        for exact, label in ((FileNotFoundError, "FileNotFoundError"),
                            (PermissionError, "PermissionError"), (ModuleNotFoundError, "ModuleNotFoundError"),
                            (ImportError, "ImportError"), (NameError, "NameError"),
                            (KeyError, "KeyError"), (ValueError, "ValueError"),
                            (TypeError, "TypeError"), (OSError, "OSError"),
                            (TimeoutError, "TimeoutError"), (RuntimeError, "RuntimeError")):
                            if type(exc) is exact:
                                output.write(label)
                                output.write("\nExecution returned an error. Use the latest "
                                             "identity and inspect existing state before "
                                             "repeating writes; correct the cause or choose "
                                             "an independent alternative.\n")
                                break
            result["output"] = output.getvalue()
            result["output_truncated"] = output.truncated
        elif request["operation"] == "inspect":
            rows = []
            for name in request["names"]:
                kind, size = "missing", None
                if name in namespace:
                    value = namespace[name]
                    value_type = type(value)
                    kind = "opaque"
                    for exact, label in ((type(None), "null"), (bool, "boolean"),
                        (int, "integer"), (float, "float"), (str, "string"),
                        (list, "list"), (dict, "dict")):
                        if value_type is exact:
                            kind = label
                            break
                    if value_type is str or value_type is list or value_type is dict:
                        size = len(value)
                rows.append({"name": name, "kind": kind, "size": size})
            result["variables"] = rows
        elif request["operation"] == "checkpoint":
            result["values"] = checkpoint_values(request["names"])
        elif request["operation"] == "export_artifact":
            result.update(export_artifact_chunk(request))
        elif request["operation"] == "restore":
            namespace.update(request["values"])
        else:
            raise ValueError()
        encoded = json.dumps(result, allow_nan=False)
        if len(encoded.encode("utf-8")) + 1 > 65536:
            raise ValueError()
    except BaseException:
        encoded = json.dumps({"id": request["id"], "ok": False, "error": "unsupported-value"})
    sys.stdout.write(encoded + "\n")
    sys.stdout.flush()
"""
