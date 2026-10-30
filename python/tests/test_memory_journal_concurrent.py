"""Deterministic journal/index interleavings; no model/network dependencies."""
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Event
import pytest
from ahc.infrastructure.memory_journal import MemoryJournal

class Embedding:
    identity = "concurrency-fixture"
    def __init__(self):
        self.entered, self.release = Event(), Event()
        self.block = False
    def embed(self, texts):
        if self.block:
            self.entered.set()
            assert self.release.wait(10), "test gate timed out"
        return [[1.0, 0.0] for _ in texts]

class Projection:
    def __init__(self):
        self.items = {}
        self.entered, self.release = Event(), Event()
        self.block = False
    def upsert(self, **item):
        self.items[item["identifier"]] = item
    def upsert_many(self, items):
        if self.block:
            self.entered.set()
            assert self.release.wait(10), "test gate timed out"
        for item in items:
            self.upsert(**item)
    def delete(self, *, identifier, scope):
        self.items.pop(identifier, None)
    def query(self, **kwargs):
        return [{"id": k, "content_digest": v["content_digest"]} for k,v in self.items.items()]

def obs(text):
    return {"kind":"fact", "summary":text, "detail":{}}

def state(memory, ref):
    with sqlite3.connect(memory.database) as db:
        return db.execute("SELECT retired,indexed FROM records WHERE ref=?", (ref,)).fetchone()

@pytest.mark.parametrize("entrypoint", ["sync", "store"])
def test_original_store_during_blocked_inference(tmp_path, entrypoint):
    embedding, projection = Embedding(), Projection()
    memory = MemoryJournal(tmp_path, embedder=embedding, projection=projection, create=True)
    if entrypoint == "sync":
        memory.store(scope="main", operation_id="old", observation=obs("old"), index=False)
    embedding.block = True
    with ThreadPoolExecutor(max_workers=2) as pool:
        work = pool.submit(memory.sync, scope="main") if entrypoint == "sync" else pool.submit(memory.store, scope="main", operation_id="old", observation=obs("old"))
        assert embedding.entered.wait(5)
        try:
            written = pool.submit(memory.store, scope="main", operation_id="new", observation=obs("new"))
            saved = written.result(timeout=2)
            assert state(memory, saved["record_ref"]) == (0,0)
            assert memory.get(scope="main", record_ref=saved["record_ref"])["record"]["summary"] == "new"
        finally:
            embedding.release.set()
        work.result(timeout=5)
    embedding.block = False
    memory.sync(scope="main")
    assert state(memory, saved["record_ref"]) == (0,1)

@pytest.mark.parametrize("stored", [False, True])
def test_concurrent_retirement_cannot_be_acknowledged_by_stale_index(tmp_path, stored):
    embedding, projection = Embedding(), Projection()
    memory = MemoryJournal(tmp_path, embedder=embedding, projection=projection, create=True)
    saved = memory.store(scope="main", operation_id="old", observation=obs("old"), index=stored)
    if stored:
        with memory._writer(), memory._connect() as db:
            db.execute("UPDATE records SET indexed=0")
        projection.block = True
        entered, release = projection.entered, projection.release
    else:
        embedding.block = True
        entered, release = embedding.entered, embedding.release
    with ThreadPoolExecutor(max_workers=2) as pool:
        work = pool.submit(memory.sync, scope="main")
        assert entered.wait(5)
        try:
            retired = pool.submit(memory.revise, scope="main", operation_id="retire", expected_ref=saved["record_ref"])
            assert retired.result(timeout=2)["status"] == "succeeded"
        finally:
            release.set()
        work.result(timeout=5)
    assert state(memory, saved["record_ref"]) == (1,0)
    assert memory.search(scope="main", query="old") == []
    memory.sync(scope="main")
    assert state(memory, saved["record_ref"]) == (1,1)
    assert not projection.items


def test_delayed_store_cannot_resurrect_completed_retirement(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from threading import current_thread
    embedding, projection = Embedding(), Projection()
    memory = MemoryJournal(tmp_path, embedder=embedding, projection=projection, create=True)
    entered, release = Event(), Event()
    actual = memory._projection_writer
    @contextmanager
    def delayed_projection():
        if current_thread().name.startswith("stale-store"):
            entered.set()
            assert release.wait(10)
        with actual():
            yield
    monkeypatch.setattr(memory, "_projection_writer", delayed_projection)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="stale-store") as pool:
        work = pool.submit(memory.store, scope="main", operation_id="late", observation=obs("late"))
        assert entered.wait(5)
        try:
            with sqlite3.connect(memory.database) as db:
                ref = db.execute("SELECT ref FROM records WHERE operation_id='late'").fetchone()[0]
            memory.revise(scope="main", operation_id="retire-late", expected_ref=ref)
            memory.sync(scope="main")
            assert state(memory, ref) == (1,1)
        finally:
            release.set()
        work.result(timeout=5)
    assert state(memory, ref) == (1,1)
    assert not projection.items, "stale Store resurrected completed retirement"
    assert memory.sync(scope="main") == 0
