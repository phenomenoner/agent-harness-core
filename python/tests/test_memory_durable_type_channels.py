import json
import pytest
from test_memory_journal import memory, observation


def typed(memory, scope="main"):
    with memory._connect() as db:
        return memory._durable_channels(db, scope, [1.0, 0.0], False)


def test_typed_vector_recovers_search_and_pack_when_external_window_misses(memory):
    target = memory.store(scope="main", operation_id="durable", observation=observation("Valve pressure must stay below the calibrated limit"))
    memory.projection.query = lambda **kw: []
    memory.reranker = type("Ranker", (), {"rerank": lambda self, query, items: items})()
    assert any("Valve pressure" in x["summary"] for x in memory.search("safe operating pressure", scope="main", limit=5))
    pack = memory.pack(query="safe operating pressure", scope="main", limit=5, budget_tokens=8000)
    assert target["record_ref"] in [x["recordRef"] for x in pack["context_pack"]["items"]]


def test_foreign_retired_notes_and_bad_chunk_digests_cannot_enter(memory):
    good=memory.store(scope="main", operation_id="good", observation=observation("Durable operating instruction"))
    foreign=memory.store(scope="other", operation_id="private", observation=observation("Private foreign preference"))
    bad=memory.store(scope="main", operation_id="bad", observation=observation("Bad chunk candidate"))
    retired=memory.store(scope="main", operation_id="retired", observation=observation("Retired candidate"))
    memory.store(scope="main", operation_id="raw", observation={"kind":"note","summary":"Raw historical note","detail":{}})
    with memory._connect() as db:
        db.execute("UPDATE records SET retired=1 WHERE ref=?", (retired["record_ref"],))
        db.execute("UPDATE chunks SET content_digest='invalid' WHERE record_ref=?", (bad["record_ref"],))
    items=[item for channel in typed(memory) for item in channel]
    assert [item["summary"] for item in items]==[good["record"]["summary"]]


@pytest.mark.parametrize("vector", [[1.0], [float("nan"), 0.0], [True, 0.0], [0.0, 0.0], "not-a-vector"])
def test_invalid_vectors_are_not_ranked(memory, vector):
    r=memory.store(scope="main", operation_id="x", observation=observation("Malformed vector candidate"))
    with memory._connect() as db:
        db.execute("UPDATE chunks SET vector=? WHERE record_ref=?", (json.dumps(vector),r["record_ref"]))
    assert all(not channel for channel in typed(memory))


def test_more_than_bounded_rows_is_optional_noop(memory):
    class DB:
        def execute(self, *args): return self
        def fetchall(self): return [None]*4097
    assert memory._durable_channels(DB(), "main", [1.0,0.0],False)==[]


def test_failed_optional_lane_preserves_original_success(memory):
    memory.store(scope="main", operation_id="visible", observation=observation("Visible original candidate"))
    memory.reranker=type("Ranker", (), {"rerank": lambda self, query, items: items})()
    def failed(*args): raise TimeoutError("optional unavailable")
    memory._durable_channels=failed
    assert memory.search("visible",scope="main",limit=5)[0]["summary"]=="Visible original candidate"


def test_curated_historical_note_remains_available_without_raw_history(memory):
    target=memory.store(scope="main",operation_id="curated",observation={"kind":"historical-note","summary":"The observatory uses red-safe lamps in its dark room","detail":{}})
    memory.store(scope="main",operation_id="raw-document",observation={"kind":"legacy-document","summary":"Uncurated historical reference","detail":{}})
    items=[item for channel in typed(memory) for item in channel]
    assert [item["summary"] for item in items]==[target["record"]["summary"]]


def test_missing_kind_index_never_uses_scope_scan(memory):
    import sqlite3
    memory.store(scope="main", operation_id="typed-only", observation=observation("Safe durable instruction"))
    with memory._connect() as db:
        db.execute("DROP INDEX IF EXISTS records_scope_retired_kind_id")
        with pytest.raises(sqlite3.OperationalError, match="no such index"):
            memory._durable_channels(db, "main", [1.0, 0.0], False)


def test_missing_kind_index_preserves_basic_search(memory):
    memory.store(scope="main", operation_id="ordinary", observation=observation("Spectrometer calibration"))
    memory.reranker = type("Ranker", (), {"rerank": lambda self, query, items: items})()
    with memory._connect() as db:
        db.execute("DROP INDEX IF EXISTS records_scope_retired_kind_id")
    assert memory.search("Spectrometer calibration", scope="main", limit=5) == [{"id": 1, "summary": "Spectrometer calibration"}]
