"""Content identity of the selected memory adapter and installed dependency route."""

import hashlib
from importlib.metadata import distribution, version
from pathlib import Path

from ahc.domain.skill_values import digest_json


def mempalace_identity():
    provider = Path(distribution("mempalace").locate_file("mempalace"))
    adapter = Path(__file__).parent
    files = {}
    for label, root, names in (
        (
            "mempalace",
            provider,
            (
                "backends/qdrant.py",
                "backends/base.py",
                "backends/_sidecar.py",
                "config.py",
                "version.py",
            ),
        ),
        (
            "ahc",
            adapter.parent,
            (
                "adapters/local_embedding.py",
                "adapters/local_reranker.py",
                "infrastructure/memory_query_views.py",
                "adapters/mempalace_projection.py",
                "adapters/mempalace_memory_worker.py",
                "infrastructure/memory_journal.py",
                "infrastructure/memory_legacy_references.py",
                "infrastructure/memory_indexing.py",
                "infrastructure/memory_keyword_search.py",
            ),
        ),
    ):
        for name in names:
            files[label + "/" + name] = hashlib.sha256((root / name).read_bytes()).hexdigest()
    dependencies = {
        name: version(name) for name in ("mempalace", "onnxruntime", "tokenizers", "numpy")
    }
    return digest_json(
        {"schema": "ahc.mempalace-implementation.v1", "files": files, "dependencies": dependencies}
    )
