"""One isolated memory operation; explicit loopback index and offline embedding."""

import contextlib
import hashlib
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

from ahc.adapters.local_embedding import MODEL_IDENTITY, LocalEmbedding
from ahc.adapters.mempalace_identity import mempalace_identity
from ahc.adapters.mempalace_projection import MemPalaceProjection
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.memory_journal import MemoryJournal
from ahc.adapters.local_reranker import LocalReranker


class LazyEmbedding:
    identity = MODEL_IDENTITY

    def __init__(self, root):
        self.root, self.loaded = root, None

    def embed(self, texts):
        if self.loaded is None:
            self.loaded = LocalEmbedding(self.root)
        return self.loaded.embed(texts)

    def split_text(self, text):
        if self.loaded is None:
            self.loaded = LocalEmbedding(self.root)
        return self.loaded.split_text(text)


class LazyProjection:
    def __init__(self, config):
        self.config, self.loaded = config, None

    def __getattr__(self, name):
        if name not in ("upsert", "query", "delete"):
            raise AttributeError(name)
        if self.loaded is None:
            config = self.config
            key = Path(config["credential_file"]).read_text(encoding="utf-8").strip()
            if not 16 <= len(key) <= 512:
                raise ValueError("invalid memory service credential")
            palace = Path(config["root"]) / "palace"
            palace.mkdir(exist_ok=True)
            self.loaded = MemPalaceProjection(
                palace_root=palace,
                palace_id=config["palace_id"],
                url=config["qdrant_url"],
                api_key=key,
                model_identity=MODEL_IDENTITY,
                timeout=min(20, config["timeout"]),
            )
        return getattr(self.loaded, name)


def main():
    raw, entered = b"", False
    try:
        raw = sys.stdin.buffer.read(70001)
        if len(raw) > 70000:
            raise ValueError()
        request = json.loads(raw)
        config, operation, options = request["config"], request["operation"], request["options"]
        if (
            config["provider_digest"] != mempalace_identity()
            or config["model_identity"] != MODEL_IDENTITY
        ):
            raise ValueError()
        fields = {
            "status": set(),
            "integrity_check": set(),
            "get": {"scope", "record_ref"},
            "resolve": {"scope", "row_id"},
            "store": {"scope", "operation_id", "observation"},
            "reconcile": {"scope", "operation_id", "request_digest"},
            "search": {"scope", "query", "limit"},
            "sync": {"scope", "limit"},
            "pack": {"scope", "query", "budget_tokens", "limit"},
            "revise": {"scope", "operation_id", "expected_ref", "observation"},
            "backup": {"destination"},
            "inspect_backup": {"destination", "expected_digest"},
        }
        if (
            operation not in fields
            or type(options) is not dict
            or set(options) != fields[operation]
        ):
            raise ValueError()
        if "scope" in options and options["scope"] != config["scope"]:
            raise ValueError()
        target = urlsplit(config["qdrant_url"])
        if target.scheme != "http" or target.hostname != "127.0.0.1" or not target.port:
            raise ValueError()

        def confined(event, args):
            if event in ("subprocess.Popen", "os.system", "socket.getaddrinfo"):
                if (
                    event == "socket.getaddrinfo"
                    and args[0] == "127.0.0.1"
                    and args[1] == target.port
                ):
                    return
                raise RuntimeError("memory worker external route forbidden")
            if event == "socket.connect" and args[1] != ("127.0.0.1", target.port):
                raise RuntimeError("memory worker external route forbidden")

        sys.addaudithook(confined)
        with (
            open(os.devnull, "w") as sink,
            contextlib.redirect_stdout(sink),
            contextlib.redirect_stderr(sink),
        ):
            entered = True
            reranker = LocalReranker(Path(__file__).resolve().parents[1] / "models" / "memory-reranker")
            if operation in {"search", "pack"}:
                reranker._load()
            memory = MemoryJournal(
                config["root"],
                embedder=LazyEmbedding(config["model_root"]),
                projection=LazyProjection(config),
                reranker=reranker,
            )
            value = getattr(memory, operation)(**options)
        encoded = canonical_json(value)
        if len(encoded) > 131072:
            raise ValueError()
        sys.stdout.buffer.write(encoded)
    except Exception:
        value = (
            {"transport": "not-dispatched", "request_digest": hashlib.sha256(raw).hexdigest()}
            if not entered
            else {"transport": "unavailable"}
        )
        sys.stdout.buffer.write(json.dumps(value).encode())
        raise SystemExit(2) from None
