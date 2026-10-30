"""Single-writer memory originals and resumable derived-index custody.

The journal is the durable authority. Qdrant is a rebuildable search projection;
its timeout cannot duplicate an original record or authorize replay of a task.
"""

import json
import re
import sqlite3
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

import portalocker

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json

from . import memory_indexing
from . import memory_legacy_references as legacy_refs

_SCHEMA = """
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE records(
 id INTEGER PRIMARY KEY, ref TEXT UNIQUE NOT NULL, scope TEXT NOT NULL,
 operation_id TEXT NOT NULL, request_digest TEXT NOT NULL, observation TEXT NOT NULL,
 created_at TEXT NOT NULL, parent TEXT, retired INTEGER NOT NULL DEFAULT 0,
 indexed INTEGER NOT NULL DEFAULT 0, UNIQUE(scope,operation_id));
CREATE INDEX records_scope_retired_kind_id ON records(
 scope, retired, json_extract(observation,'$.kind'), id);
CREATE TABLE chunks(
 id TEXT PRIMARY KEY, record_ref TEXT NOT NULL REFERENCES records(ref),
 content TEXT NOT NULL, content_digest TEXT NOT NULL, vector TEXT NOT NULL);
CREATE TABLE mutations(scope TEXT NOT NULL, operation_id TEXT NOT NULL,
 request_digest TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(scope, operation_id));
"""


def _scope(value):
    if type(value) is not str or not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]{0,255}", value):
        raise ContractError("memory scope invalid")


def _operation(value):
    if type(value) is not str or not 1 <= len(value) <= 256 or not value.strip():
        raise ContractError("memory operation identity invalid")


class MemoryJournal:
    def __init__(self, root, *, embedder, projection, create=False, reranker=None):
        self.root = Path(root)
        if self.root.is_symlink():
            raise ContractError("memory journal root cannot be a link")
        if create:
            self.root.mkdir(parents=True, exist_ok=True)
        self.database = self.root / "memory.db"
        self.lock = self.root / "writer.lock"
        self.embedder, self.projection = embedder, projection
        self.reranker = reranker
        if self.database.is_symlink():
            raise ContractError("memory journal cannot be a link")
        if not self.database.is_file():
            if not create:
                raise ContractError("memory journal is not initialized")
            with self._writer():
                with self._connect() as db:
                    if not db.execute(
                        "SELECT name FROM sqlite_master WHERE name='meta'"
                    ).fetchone():
                        db.executescript(_SCHEMA)
                        db.executemany(
                            "INSERT INTO meta VALUES (?,?)",
                            (
                                ("schema", "ahc.memory-journal.v1"),
                                ("store_id", str(uuid.uuid4())),
                                ("embedding", embedder.identity),
                            ),
                        )
        with self._connect() as db:
            metadata = dict(db.execute("SELECT key,value FROM meta"))
            if (
                metadata.get("schema") != "ahc.memory-journal.v1"
                or metadata.get("embedding") != embedder.identity
            ):
                raise ContractError("memory journal identity mismatch")
            self.store_id = metadata["store_id"]
        if create:
            with self._writer(), self._connect() as db:
                legacy_refs.initialize(db)
                # Prepare only under explicit initialization and writer custody.
                # Ordinary reopen/status must not mutate a legacy journal.
                db.execute(
                    "CREATE INDEX IF NOT EXISTS records_pending_status "
                    "ON records(indexed) WHERE indexed=0"
                )

    @contextmanager
    def _writer(self):
        with portalocker.Lock(
            str(self.lock), flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
            timeout=5, check_interval=0.01
        ):
            yield

    @contextmanager
    def _projection_writer(self):
        # Optional derived work must never block custody of an original.
        with portalocker.Lock(
            str(self.root / "projection.lock"),
            flags=portalocker.LOCK_EX | portalocker.LOCK_NB, timeout=0,
        ):
            yield

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.database, timeout=1)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _observation(scope, observation):
        if (
            type(observation) is not dict
            or set(observation) != {"kind", "summary", "detail"}
            or any(
                type(observation[k]) is not str or not observation[k].strip()
                for k in ("kind", "summary")
            )
            or type(observation["detail"]) is not dict
            or observation["detail"].get("scope", scope) != scope
        ):
            raise ContractError("memory observation or scope invalid")
        request = {"version": 1, "scope": scope, "observation": observation}
        if len(canonical_json(request)) > 65536:
            raise ContractError("memory observation exceeds bound")
        return digest_json(request)

    def _record(self, row):
        observation = json.loads(row["observation"])
        if self._observation(row["scope"], observation) != row["request_digest"]:
            raise ContractError("memory journal record integrity mismatch")
        return {
            "status": "succeeded",
            "record_ref": row["ref"],
            "request_digest": row["request_digest"],
            "record": {
                **observation,
                "detail": {**observation["detail"], "scope": row["scope"]},
                "created_at": row["created_at"],
            },
            "provenance": {"origin": "unknown"},
            "trust": "untrusted",
            "trace": {"operation_id": row["operation_id"], "request_digest": row["request_digest"]},
        }

    def store(self, *, scope, operation_id, observation, index=True):
        if type(index) is not bool:
            raise ContractError("memory projection choice invalid")
        _scope(scope)
        _operation(operation_id)
        request_digest = self._observation(scope, observation)
        with self._writer():
            with self._connect() as db:
                previous = legacy_refs.operation(db, scope, operation_id, request_digest)
                if previous is not None:
                    return previous
                if db.execute(
                    "SELECT 1 FROM mutations WHERE scope=? AND operation_id=?",
                    (scope, operation_id),
                ).fetchone():
                    raise ContractError("memory operation already used by a mutation")
                row = db.execute(
                    "SELECT * FROM records WHERE scope=? AND operation_id=?", (scope, operation_id)
                ).fetchone()
                if row is not None:
                    if row["request_digest"] != request_digest:
                        raise ContractError("memory operation payload changed")
                else:
                    ref = "ahcm:" + digest_json([self.store_id, scope, operation_id])
                    db.execute(
                        "INSERT INTO records(ref,scope,operation_id,request_digest,"
                        "observation,created_at) VALUES(?,?,?,?,?,?)",
                        (
                            ref,
                            scope,
                            operation_id,
                            request_digest,
                            canonical_json(observation).decode(),
                            datetime.now(UTC).isoformat(),
                        ),
                    )
                    row = db.execute("SELECT * FROM records WHERE ref=?", (ref,)).fetchone()
        # Commit and release original custody before inference/network. A busy
        # projection leaves the durable record pending for a later bounded sync.
        if index:
            try:
                with self._projection_writer():
                    self._index(row)
            except Exception:
                pass
        return self._record(row)

    def reconcile(self, *, scope, operation_id, request_digest):
        _scope(scope)
        _operation(operation_id)
        with self._connect() as db:
            previous = legacy_refs.operation(db, scope, operation_id, request_digest)
            if previous is not None:
                return previous
            row = db.execute(
                "SELECT * FROM records WHERE scope=? AND operation_id=?", (scope, operation_id)
            ).fetchone()
        if row is None:
            # A possibly executing concurrent request may still commit.
            return {"status": "unknown"}
        if row["request_digest"] != request_digest:
            raise ContractError("memory reconciliation payload mismatch")
        return self._record(row)

    def get(self, *, scope, record_ref):
        _scope(scope)
        with self._connect() as db:
            previous = legacy_refs.get(db, scope, record_ref)
            if previous is not None:
                return previous
            row = db.execute(
                "SELECT * FROM records WHERE scope=? AND ref=? AND retired=0", (scope, record_ref)
            ).fetchone()
        return {"status": "not-found"} if row is None else self._record(row)

    def resolve(self, *, scope, row_id):
        _scope(scope)
        if type(row_id) is not int or row_id <= 0:
            raise ContractError("memory citation invalid")
        with self._connect() as db:
            row = db.execute(
                "SELECT ref FROM records WHERE scope=? AND id=? AND retired=0", (scope, row_id)
            ).fetchone()
        return (
            {"status": "not-found"}
            if row is None
            else {"status": "succeeded", "record_ref": row[0]}
        )

    def status(self):
        """Operational/schema/index readiness, explicitly not a full-store check."""
        with self._connect() as db:
            pending = db.execute("SELECT count(*) FROM records WHERE indexed=0").fetchone()[0]
            policy = db.execute("SELECT value FROM meta WHERE key='index_policy'").fetchone()
            current = (policy is not None and policy[0] == memory_indexing.INDEX_POLICY) or (
                policy is None and not db.execute("SELECT 1 FROM chunks LIMIT 1").fetchone()
            )
            from . import memory_keyword_search as keywords
            scopes=[row[0] for row in db.execute('SELECT DISTINCT scope FROM records')]
            lexical=bool(scopes) and all(keywords.ready(db,scope) for scope in scopes)
        vector_ready = not pending and current
        ready = vector_ready or lexical
        return {
            "schema_version": "ahc.memory.native.status.v3",
            "status": "ready" if ready else "unavailable",
            "code": ("native-schema-supported" if vector_ready else
                     "native-keyword-ready" if lexical else "storage-unverified"),
            "observation_scope": "operational",
            "integrity": "not-checked",
            "capabilities": {
                name: True
                for name in ("observe_status", "observe_get", "store_committed", "reconcile_store")
            },
        }

    def integrity_check(self):
        """Explicit read-only whole SQLite quick-check; not vector/recovery proof."""
        with closing(sqlite3.connect(
            # SQLite's mode=ro can skip CHECK-constraint validation. mode=rw
            # requires an existing file; query_only enforces the no-write probe.
            self.database.absolute().as_uri() + "?mode=rw", uri=True, timeout=1
        )) as db:
            db.execute("PRAGMA query_only=ON")
            valid = db.execute("PRAGMA quick_check").fetchall() == [("ok",)]
        return {
            "schema_version": "ahc.memory.native.integrity.v1",
            "status": "ok" if valid else "failed",
            "check": "sqlite-quick-check",
            "observation_scope": "whole-database",
        }

    def _index(self, row):
        # Callers hold projection custody. Refresh after acquiring it: an earlier
        # Store snapshot may predate an already completed retirement cleanup.
        with self._writer(), self._connect() as db:
            row = db.execute("SELECT * FROM records WHERE ref=?", (row["ref"],)).fetchone()
            if row is None:
                raise ContractError("memory index original disappeared")
            if row["indexed"]:
                return
            self._record(row)
            memory_indexing.prepare(db)
            chunks = db.execute(
                "SELECT * FROM chunks WHERE record_ref=? ORDER BY id", (row["ref"],)
            ).fetchall()
        if row["retired"]:
            for chunk in chunks:
                self.projection.delete(identifier=chunk["id"], scope=row["scope"])
        else:
            if not chunks:
                observation = self._record(row)["record"]
                text = memory_indexing.semantic_text(observation)
                prepared = []
                splitter = getattr(self.embedder, "split_text", None)
                parts = (
                    splitter(text)
                    if callable(splitter)
                    else [text[i : i + 1500] for i in range(0, len(text), 1500)]
                )
                if (
                    type(parts) is not list
                    or not parts
                    or any(type(part) is not str or not part for part in parts)
                    or "".join(parts) != text
                ):
                    raise ContractError("embedding chunks must preserve complete source text")
                for ordinal, part in enumerate(parts):
                    with self._connect() as db:
                        vector = memory_indexing.cached_vector(
                            db,
                            scope=row["scope"],
                            text=part,
                            embedding_identity=self.embedder.identity,
                        )
                    if vector is None:
                        vector = self.embedder.embed([part])[0]
                        with self._writer(), self._connect() as db:
                            memory_indexing.save_vector(
                                db,
                                scope=row["scope"],
                                text=part,
                                vector=vector,
                                embedding_identity=self.embedder.identity,
                            )
                    identifier = row["ref"] + ":" + str(ordinal)
                    prepared.append(
                        (
                            identifier,
                            row["ref"],
                            part,
                            digest_json(part),
                            canonical_json(vector).decode(),
                        )
                    )
                with self._writer(), self._connect() as db:
                    db.executemany("INSERT INTO chunks VALUES(?,?,?,?,?)", prepared)
                with self._connect() as db:
                    chunks = db.execute(
                        "SELECT * FROM chunks WHERE record_ref=? ORDER BY id", (row["ref"],)
                    ).fetchall()
            for chunk in chunks:
                self.projection.upsert(
                    identifier=chunk["id"],
                    scope=row["scope"],
                    text=chunk["content"],
                    vector=json.loads(chunk["vector"]),
                    content_digest=chunk["content_digest"],
                )
        with self._writer(), self._connect() as db:
            db.execute(
                "UPDATE records SET indexed=1 WHERE ref=? AND retired=?",
                (row["ref"], row["retired"]),
            )

    def _sync_stored(self, rows):
        """Rebuild stored vectors under projection custody, outside writer custody.

        Originals/chunks are already durable. Failed/partial external upserts
        leave every selected record pending, so only derived points are retried.
        Fresh embeddings, retirement and large batches retain the original path.
        """
        bulk = getattr(self.projection, "upsert_many", None)
        if not rows:
            return True
        if not callable(bulk) or any(row["retired"] for row in rows):
            return False
        items = []
        with self._connect() as db:
            for row in rows:
                self._record(row)  # Verify original integrity before external effects.
                chunks = db.execute(
                    "SELECT * FROM chunks WHERE record_ref=? ORDER BY id LIMIT 257",
                    (row["ref"],),
                ).fetchall()
                if not chunks or len(items) + len(chunks) > 256:
                    return False
                for chunk in chunks:
                    if digest_json(chunk["content"]) != chunk["content_digest"]:
                        raise ContractError("memory chunk integrity mismatch")
                    items.append({"identifier": chunk["id"], "scope": row["scope"],
                        "text": chunk["content"], "vector": json.loads(chunk["vector"]),
                        "content_digest": chunk["content_digest"]})
        for offset in range(0, len(items), 64):
            bulk(items[offset:offset + 64])
        # No indexed flag commits before every batch readback has succeeded.
        with self._writer(), self._connect() as db:
            db.executemany("UPDATE records SET indexed=1 WHERE ref=? AND retired=0",
                           [(row["ref"],) for row in rows])
        return True

    def sync(self, *, scope, limit=16):
        _scope(scope)
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ContractError("memory sync bound invalid")
        with self._projection_writer():
            with self._writer(), self._connect() as db:
                memory_indexing.prepare(db)
                rows = db.execute(
                    "SELECT * FROM records WHERE scope=? AND indexed=0 ORDER BY id LIMIT ?",
                    (scope, limit),
                ).fetchall()
            if not self._sync_stored(rows):
                for row in rows:
                    self._index(row)
            return len(rows)

    def search(self, query, *, scope, limit=8, **options):
        _scope(scope)
        if (
            type(query) is not str
            or not query.strip()
            or len(query) > 4096
            or type(limit) is not int
            or not 1 <= limit <= 64
        ):
            raise ContractError("memory search bounds invalid")
        from . import memory_keyword_search as keywords
        with self._connect() as db:
            lexical_ready=keywords.ready(db,scope)
            pending=db.execute('SELECT 1 FROM records WHERE scope=? AND indexed=0 LIMIT 1',
                               (scope,)).fetchone()
            partial = lexical_ready and pending is not None
            if partial:
                found = self._keyword_results(keywords.search(db,query,scope,limit))
                if found:
                    return found
        from .memory_query_views import query_views
        views = query_views(query)
        try:
            # Incomplete projections must not trigger expensive read maintenance.
            if not partial:
                self.sync(scope=scope)
            query_vector = self.embedder.embed([query])[0]
            hits = self.projection.query(
                scope=scope, vector=query_vector, limit=256
            )
        except Exception:
            if not lexical_ready:
                raise
            with self._connect() as db:
                return self._keyword_results(keywords.search(db,query,scope,limit))
        hit_channels = [hits]
        for view in views[1:]:
            try:
                hit_channels.append(self.projection.query(
                    scope=scope, vector=self.embedder.embed([view])[0], limit=256
                ))
            except Exception:
                # Optional reformulation cannot erase a successful original read.
                pass
        channels = []
        with self._connect() as db:
            channels = self._hydrate_vector_channels(db, hit_channels, scope, partial)
            if lexical_ready:
                for view in views:
                    channels.append(self._keyword_results(keywords.search(db, view, scope, 256)))
        if self.reranker is None:
            return self._rank_channels(channels, limit)
        try:
            with self._connect() as db:
                channels.extend(self._durable_channels(db, scope, query_vector, partial))
        except Exception:
            # Optional typed reads cannot erase the original successful path.
            pass
        shortlist = self._balanced_shortlist(channels, 128)
        return self._rerank_results(query, shortlist, scope, limit, partial)

    def _hydrate_vector_channels(self, db, hit_channels, scope, partial):
        """Read each bounded set once, then restore the original hit order.

        The index only proposes locators. Scoped active originals and each hit's
        chunk digest still authorize hydration; scoring rechecks them later.
        """
        identifiers = list(dict.fromkeys(hit["id"] for channel in hit_channels for hit in channel))
        rows = {}
        # Keep bounded chunk IDs as the outer loop; a scope-first plan scans history.
        for offset in range(0, len(identifiers), 256):
            batch = identifiers[offset:offset + 256]
            marks = ",".join("?" for _ in batch)
            found = db.execute(
                "SELECT chunks.id AS candidate_chunk_id, records.*, "
                "chunks.content_digest AS chunk_digest "
                "FROM chunks CROSS JOIN records ON records.ref=chunks.record_ref "
                f"WHERE chunks.id IN ({marks}) AND records.scope=? AND retired=0"
                + (" AND records.indexed=1" if partial else ""),
                (*batch, scope),
            ).fetchall()
            rows.update((row["candidate_chunk_id"], row) for row in found)
        channels, summaries = [], {}
        for channel in hit_channels:
            results, seen = [], set()
            for hit in channel:
                row = rows.get(hit["id"])
                if row is None or row["chunk_digest"] != hit["content_digest"]:
                    continue
                identifier = row["id"]
                if identifier in seen:
                    continue
                if identifier not in summaries:
                    summaries[identifier] = self._record(row)["record"]["summary"][:16384]
                results.append({"id": identifier, "summary": summaries[identifier]})
                seen.add(identifier)
            channels.append(results)
        return channels

    def _durable_channels(self, db, scope, query_vector, partial):
        """Optional typed reads of existing vectors; no facts or projections written.

        Explicit decisions/preferences are scarce compared with raw historical
        material. Let each retain a candidate lane before cross-encoder scoring.
        """
        import math
        if not query_vector or any(type(v) not in (int, float) or not math.isfinite(v)
                                   for v in query_vector):
            return []
        norm = math.sqrt(math.fsum(v * v for v in query_vector))
        if not norm:
            return []
        rows = db.execute(
            "SELECT records.*, chunks.content AS candidate_content, "
            "chunks.content_digest AS candidate_digest, chunks.vector AS candidate_vector "
            "FROM records INDEXED BY records_scope_retired_kind_id "
            "JOIN chunks ON records.ref=chunks.record_ref "
            "WHERE records.scope=? AND records.retired=0 "
            "AND json_extract(records.observation,'$.kind') IN ('decision','preference','historical-note')"
            + (" AND records.indexed=1" if partial else "")
            + " ORDER BY records.id,chunks.id LIMIT 4097", (scope,)
        ).fetchall()
        if len(rows) > 4096:
            return []
        groups = {"decision": {}, "preference": {}, "historical-note": {}}
        for row in rows:
            if digest_json(row["candidate_content"]) != row["candidate_digest"]:
                continue
            record = self._record(row)["record"]
            kind = record["kind"]
            if kind not in groups:
                continue
            vector = json.loads(row["candidate_vector"])
            if (type(vector) is not list or len(vector) != len(query_vector)
                    or any(type(v) not in (int, float) or not math.isfinite(v) for v in vector)):
                continue
            vnorm = math.sqrt(math.fsum(v * v for v in vector))
            if not vnorm:
                continue
            score = math.fsum(a * b for a, b in zip(query_vector, vector)) / (norm * vnorm)
            previous = groups[kind].get(row["id"])
            if previous is None or score > previous[0]:
                groups[kind][row["id"]] = (score, record["summary"][:16384])
        return [[{"id": identifier, "summary": summary}
                 for identifier, (score, summary) in sorted(group.items(),
                       key=lambda pair: (-pair[1][0], pair[0]))[:256]]
                for group in groups.values() if group]

    def _rerank_results(self, query, shortlist, scope, limit, partial):
        allowed = {item["id"]: dict(item) for item in shortlist}
        try:
            ranked = self.reranker.rerank(query, [dict(item) for item in shortlist])
            identifiers = [item["id"] for item in ranked]
            if len(identifiers) != len(allowed) or set(identifiers) != set(allowed):
                raise ValueError("reranker locator mismatch")
        except Exception:
            identifiers = [item["id"] for item in shortlist]
        results, disclosed = [], set()
        # Inference has no writer custody. Recheck canonical state after its
        # delay, including the fallback path; scorer text never becomes facts.
        with self._connect() as db:
            for identifier in identifiers:
                row = db.execute(
                    "SELECT * FROM records WHERE id=? AND scope=? AND retired=0"
                    + (" AND indexed=1" if partial else ""),
                    (identifier, scope),
                ).fetchone()
                if row is None:
                    continue
                summary = self._record(row)["record"]["summary"][:16384]
                text_key = " ".join(summary.casefold().split())
                if text_key in disclosed:
                    continue
                disclosed.add(text_key)
                results.append({"id": identifier, "summary": summary})
                if len(results) == limit:
                    break
        return results

    @staticmethod
    def _balanced_shortlist(channels, limit):
        # A single high-overlap modality must not suppress the other channel's
        # independently valid locators before relevance scoring.
        selected, identifiers, disclosed = [], set(), set()
        for rank in range(max((len(channel) for channel in channels), default=0)):
            for channel in channels:
                if rank >= len(channel):
                    continue
                item = channel[rank]
                text_key = " ".join(item["summary"].casefold().split())
                if item["id"] in identifiers or text_key in disclosed:
                    continue
                identifiers.add(item["id"])
                disclosed.add(text_key)
                selected.append(item)
                if len(selected) == limit:
                    return selected
        return selected

    @staticmethod
    def _rank_channels(channels, limit):
        # All channel items have already passed canonical hydration. No provider
        # text or unvalidated reformulation locators can become disclosed facts.
        scores, values, order = {}, {}, {}
        for channel in channels:
            for rank, item in enumerate(channel, 1):
                identifier = item["id"]
                scores[identifier] = scores.get(identifier, 0.0) + 1.0 / (60 + rank)
                values[identifier] = item
                order.setdefault(identifier, len(order))
        ranked = sorted(values, key=lambda identifier: (-scores[identifier], order[identifier]))
        heads = [channel[0]["id"] for channel in channels if channel] if limit > 1 else []
        selected, disclosed = [], set()
        for identifier in heads + ranked:
            item = values[identifier]
            text_key = " ".join(item["summary"].casefold().split())
            if text_key in disclosed:
                continue
            disclosed.add(text_key)
            selected.append(item)
            if len(selected) == limit:
                break
        return selected

    def _keyword_results(self, rows):
        return [{'id':row['id'],'summary':self._record(row)['record']['summary'][:16384]}
                for row in rows]

    def build_keyword_index(self, *, scope, limit=512):
        """Explicit bounded maintenance; never triggered by status or a read."""
        from . import memory_keyword_search as keywords
        _scope(scope)
        if type(limit) is not int or not 1 <= limit <= 512:
            raise ContractError('keyword build bound invalid')
        with self._writer(), self._connect() as db:
            return keywords.build(db,scope,limit)

    def revise(self, *, scope, operation_id, expected_ref, observation=None):
        """CAS correction or tombstone. Originals remain inert recovery evidence."""
        _scope(scope)
        _operation(operation_id)
        if type(expected_ref) is not str or not 1 <= len(expected_ref) <= 256:
            raise ContractError("memory predecessor reference invalid")
        if observation is not None:
            request_digest = self._observation(scope, observation)
        else:
            request_digest = None
        mutation_digest = digest_json(
            {
                "scope": scope,
                "operation_id": operation_id,
                "expected_ref": expected_ref,
                "observation": observation,
            }
        )
        with self._writer():
            with self._connect() as db:
                prior = db.execute(
                    "SELECT * FROM mutations WHERE scope=? AND operation_id=?",
                    (scope, operation_id),
                ).fetchone()
                if prior is not None:
                    if prior["request_digest"] != mutation_digest:
                        raise ContractError("memory mutation payload changed")
                    return json.loads(prior["result"])
                if db.execute(
                    "SELECT 1 FROM records WHERE scope=? AND operation_id=?", (scope, operation_id)
                ).fetchone() or (
                    legacy_refs.available(db)
                    and db.execute(
                        "SELECT 1 FROM legacy_memory_operations WHERE scope=? AND operation_id=?",
                        (scope, operation_id),
                    ).fetchone()
                ):
                    raise ContractError("memory mutation identity already used")
                canonical_ref = legacy_refs.canonical(db, scope, expected_ref)
                old = db.execute(
                    "SELECT * FROM records WHERE scope=? AND ref=? AND retired=0",
                    (scope, canonical_ref),
                ).fetchone()
                if old is None:
                    raise ContractError("memory predecessor changed or is outside scope")
                db.execute("UPDATE records SET retired=1,indexed=0 WHERE ref=?", (canonical_ref,))
                successor = None
                if observation is not None:
                    successor = "ahcm:" + digest_json([self.store_id, scope, operation_id])
                    db.execute(
                        "INSERT INTO records(ref,scope,operation_id,request_digest,"
                        "observation,created_at,parent) VALUES(?,?,?,?,?,?,?)",
                        (
                            successor,
                            scope,
                            operation_id,
                            request_digest,
                            canonical_json(observation).decode(),
                            datetime.now(UTC).isoformat(),
                            canonical_ref,
                        ),
                    )
                result = {
                    "status": "succeeded",
                    "previous_ref": expected_ref,
                    "record_ref": successor,
                    "mutation_digest": mutation_digest,
                }
                db.execute(
                    "INSERT INTO mutations VALUES(?,?,?,?)",
                    (scope, operation_id, mutation_digest, canonical_json(result).decode()),
                )
            # Logical deletion is immediate even when physical projection cleanup
            # is pending. Query results are always rechecked against this journal.
        return result

    def pack(self, *, query, scope, budget_tokens=1200, limit=8):
        if type(budget_tokens) is not int or not 1 <= budget_tokens <= 16384:
            raise ContractError("memory pack budget invalid")
        rows = self.search(query, scope=scope, limit=limit)
        items, lines = [], []
        for row in rows:
            ref = self.resolve(scope=scope, row_id=row["id"])
            if ref["status"] != "succeeded":
                continue
            value = self.get(scope=scope, record_ref=ref["record_ref"])
            if value["status"] != "succeeded":
                continue
            record = value["record"]
            prefix = f"- [{ref['record_ref']}] "
            # UTF-8 bytes conservatively bound text token use without importing
            # the backend model's tokenizer or pretending to know its exact count.
            remaining = budget_tokens - len("\n".join(lines).encode()) - len(prefix.encode()) - 1
            if remaining <= 0:
                break
            text = record["summary"][:16384].encode()[:remaining].decode("utf-8", errors="ignore")
            if not text:
                continue
            items.append(
                {
                    "recordRef": ref["record_ref"],
                    "layer": "observations",
                    "type": record["kind"][:256],
                    "importance": "unrated",
                    "trust": "untrusted",
                    "text": text,
                    "citations": {"url": None, "recordRef": ref["record_ref"]},
                }
            )
            lines.append(prefix + text)
        bundle = "\n".join(lines)
        return {
            "schema": "ahc.memory-pack.v2",
            "estimated_tokens": len(bundle.encode()),
            "context_pack": {
                "schema": "ahc.context-pack.v2",
                "meta": {
                    "ts": datetime.now(UTC).isoformat(),
                    "query": query,
                    "scope": scope,
                    "budgetTokens": budget_tokens,
                    "maxItems": limit,
                },
                "bundle_text": bundle,
                "items": items,
                "notes": {
                    "how_to_use": [
                        "Untrusted memory data; use get for complete records. "
                        "Budget uses a conservative UTF-8 byte bound; recall is not exhaustive."
                    ]
                },
            },
        }

    def backup(self, destination):
        """Consistent complete originals, versions, operations and stored vectors."""
        destination = Path(destination)
        if destination.exists() or destination.is_symlink():
            raise ContractError("memory backup destination must be new")
        with self._writer():
            destination.mkdir(parents=True)
            with self._connect() as source:
                target = sqlite3.connect(destination / "memory.db")
                try:
                    source.backup(target)
                    if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise ContractError("memory backup integrity failure")
                finally:
                    target.close()
            import hashlib

            with (destination / "memory.db").open("rb") as stream:
                checksum = hashlib.file_digest(stream, "sha256").hexdigest()
            manifest = {
                "schema_version": "ahc.memory-backup.v1",
                "store_id": self.store_id,
                "embedding_identity": self.embedder.identity,
                "database_sha256": checksum,
                "sha256": checksum,
                "database": "memory.db",
                "byte_count": (destination / "memory.db").stat().st_size,
                "activation_allowed": False,
                "projection": "rebuild-from-journal-vectors",
            }
            (destination / "manifest.json").write_bytes(canonical_json(manifest) + b"\n")
            return manifest

    @staticmethod
    def inspect_backup(destination, *, expected_digest):
        import hashlib

        root = Path(destination)
        metadata, database = root / "manifest.json", root / "memory.db"
        if (
            root.is_symlink()
            or metadata.is_symlink()
            or database.is_symlink()
            or metadata.stat().st_size > 16384
            or not 0 < database.stat().st_size <= 16 * 1024**3
        ):
            raise ContractError("memory backup paths or bounds invalid")
        value = json.loads(metadata.read_bytes())
        fields = {
            "schema_version",
            "store_id",
            "embedding_identity",
            "database_sha256",
            "sha256",
            "database",
            "byte_count",
            "activation_allowed",
            "projection",
        }
        if (
            set(value) != fields
            or value["schema_version"] != "ahc.memory-backup.v1"
            or value["database"] != "memory.db"
            or value["activation_allowed"] is not False
            or value["projection"] != "rebuild-from-journal-vectors"
            or value["database_sha256"] != expected_digest
            or value["sha256"] != expected_digest
            or value["byte_count"] != database.stat().st_size
        ):
            raise ContractError("memory backup manifest invalid")
        with database.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != expected_digest:
                raise ContractError("memory backup digest changed")
        db = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
        try:
            metadata = dict(db.execute("SELECT key,value FROM meta"))
            if (
                db.execute("PRAGMA integrity_check").fetchone()[0] != "ok"
                or metadata.get("store_id") != value["store_id"]
                or metadata.get("embedding") != value["embedding_identity"]
            ):
                raise ContractError("memory backup identity or integrity invalid")
        finally:
            db.close()
        return value

    @classmethod
    def restore(cls, backup, destination, *, embedder, projection):
        """Restore into a new generation only; never overwrite post-backup writes."""
        import hashlib
        import shutil

        backup, destination = Path(backup), Path(destination)
        manifest = json.loads((backup / "manifest.json").read_bytes())
        cls.inspect_backup(backup, expected_digest=manifest.get("database_sha256"))
        if (
            manifest.get("schema_version") != "ahc.memory-backup.v1"
            or manifest.get("embedding_identity") != embedder.identity
            or destination.exists()
            or destination.is_symlink()
        ):
            raise ContractError("memory restore target or identity invalid")
        with (backup / "memory.db").open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["database_sha256"]:
                raise ContractError("memory backup digest changed")
        destination.mkdir(parents=True)
        shutil.copyfile(backup / "memory.db", destination / "memory.db")
        with (destination / "memory.db").open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["database_sha256"]:
                raise ContractError("memory backup changed while restoring")
        restored = cls(destination, embedder=embedder, projection=projection)
        if restored.store_id != manifest["store_id"]:
            raise ContractError("memory restored store identity mismatch")
        with restored._writer(), restored._connect() as db:
            if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ContractError("memory restored journal integrity failure")
            db.execute("UPDATE records SET indexed=0")
        return restored
