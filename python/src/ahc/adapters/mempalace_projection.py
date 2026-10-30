"""Explicit loopback MemPalace/Qdrant projection; records remain journal-owned."""

import ipaddress
from urllib.parse import urlsplit

from ahc.domain.sessions import ContractError


class MemPalaceProjection:
    def __init__(self, *, palace_root, palace_id, url, api_key, model_identity, timeout=10,
                 create=True):
        from mempalace.backends.base import EmbedderIdentity, PalaceRef
        from mempalace.backends.qdrant import QdrantBackend

        parsed = urlsplit(url)
        try:
            local = ipaddress.ip_address(parsed.hostname).is_loopback
        except (ValueError, TypeError):
            local = False
        if (
            parsed.scheme != "http"
            or not local
            or not parsed.port
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
            or type(api_key) is not str
            or not api_key
            or not 0 < timeout <= 120
            or type(create) is not bool
        ):
            raise ContractError(
                "memory projection requires an explicit authenticated loopback target"
            )
        self.backend = QdrantBackend()
        try:
            self.collection = self.backend.get_collection(
                palace=PalaceRef(id=palace_id, local_path=str(palace_root), namespace="ahc-memory"),
                collection_name="records",
                create=create,
                options={"url": url, "api_key": api_key, "timeout": timeout,
                         "namespace": "ahc-memory"},
            )
            expected = EmbedderIdentity(model_identity, 384)
            stored = self.collection.get_stored_embedder_identity()
            if (not create and stored is None) or (stored is not None and stored != expected):
                raise ContractError("memory projection embedding identity absent or changed")
            if create:
                self.collection.set_embedder_identity(expected)
        except BaseException:
            self.backend.close()
            raise

    def upsert(self, *, identifier, scope, text, vector, content_digest):
        metadata = {"scope": scope, "content_digest": content_digest}
        self.collection.upsert(
            ids=[identifier], documents=[text], embeddings=[vector], metadatas=[metadata]
        )
        observed = self.collection.get(
            ids=[identifier], where={"scope": scope}, include=["documents", "metadatas"]
        )
        if (
            observed.ids != [identifier]
            or observed.documents != [text]
            or len(observed.metadatas) != 1
            or any(observed.metadatas[0].get(k) != v for k, v in metadata.items())
        ):
            raise ContractError("memory projection readback mismatch")

    def upsert_many(self, items):
        """Bounded same-scope derived points; verify all acknowledgments by readback."""
        fields = {"identifier", "scope", "text", "vector", "content_digest"}
        if (
            type(items) not in (list, tuple) or not 1 <= len(items) <= 64
            or any(type(item) is not dict or set(item) != fields for item in items)
        ):
            raise ContractError("memory projection batch invalid")
        items = [dict(item) for item in items]
        scope = items[0]["scope"]
        identifiers = [item["identifier"] for item in items]
        if (
            type(scope) is not str or not scope
            or any(item["scope"] != scope for item in items)
            or any(type(key) is not str or not key for key in identifiers)
            or len(set(identifiers)) != len(identifiers)
        ):
            raise ContractError("memory projection batch scope or identity invalid")
        self.collection.upsert(
            ids=identifiers, documents=[item["text"] for item in items],
            embeddings=[item["vector"] for item in items],
            metadatas=[{"scope": scope, "content_digest": item["content_digest"]} for item in items],
        )
        observed = self.collection.get(
            ids=identifiers, where={"scope": scope}, include=["documents", "metadatas"]
        )
        if (
            len(observed.ids) != len(items) or len(set(observed.ids)) != len(items)
            or set(observed.ids) != set(identifiers)
            or len(observed.documents) != len(items) or len(observed.metadatas) != len(items)
        ):
            raise ContractError("memory projection batch readback mismatch")
        expected = {item["identifier"]: item for item in items}
        for key, text, metadata in zip(observed.ids, observed.documents, observed.metadatas, strict=True):
            item = expected[key]
            if (
                text != item["text"] or type(metadata) is not dict
                or metadata.get("scope") != scope
                or metadata.get("content_digest") != item["content_digest"]
            ):
                raise ContractError("memory projection batch readback mismatch")

    def query(self, *, scope, vector, limit):
        found = self.collection.query(
            query_embeddings=[vector],
            where={"scope": scope},
            n_results=limit,
            include=["metadatas", "distances"],
        )
        rows = []
        for identifier, meta, distance in zip(
            found.ids[0], found.metadatas[0], found.distances[0], strict=True
        ):
            if meta.get("scope") != scope:
                raise ContractError("memory projection scope violation")
            rows.append(
                {
                    "id": identifier,
                    "content_digest": meta.get("content_digest"),
                    "distance": distance,
                }
            )
        return rows

    def delete(self, *, identifier, scope):
        self.collection.delete(ids=[identifier], where={"scope": scope})
        if self.collection.get(ids=[identifier], where={"scope": scope}).ids:
            raise ContractError("memory projection deletion unverified")

    def close(self):
        self.backend.close()
