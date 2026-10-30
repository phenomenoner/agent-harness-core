"""Scoped vector hydration keeps channel order without per-hit SQL round trips."""
from contextlib import contextmanager

from test_memory_journal import memory, observation


def test_search_batches_repeated_vector_hits_without_changing_rank_order(memory, monkeypatch):
    for index in range(32):
        memory.store(scope="main", operation_id=f"calibration-{index}",
                     observation=observation(f"Spectrometer calibration station {index}"))
    hits=[{"id":key,"content_digest":value["content_digest"]}
          for key,value in reversed(list(memory.projection.items.items()))]
    memory.projection.query=lambda **kwargs:list(hits)
    statements=[]
    original=memory._connect
    @contextmanager
    def traced():
        with original() as db:
            db.set_trace_callback(statements.append)
            yield db
    monkeypatch.setattr(memory,"_connect",traced)
    actual=memory.search("How should Nora calibrate the optical spectrometer wavelength?",scope="main",limit=5)
    assert [row["id"] for row in actual]==[32,31,30,29,28]
    reads=[sql for sql in statements if "FROM chunks" in sql and "JOIN records" in sql]
    assert len(reads)<=2, f"repeated per-hit hydration: {len(reads)} SQL round trips"


def test_bad_digest_hit_does_not_hide_a_later_valid_hit(memory):
    owned=memory.store(scope="main",operation_id="owned",observation=observation("Calibration ledger"))
    good=next(iter(memory.projection.query(scope="main",vector=[1,0],limit=256)))
    memory.projection.query=lambda **kwargs:[{**good,"content_digest":"stale"},dict(good),dict(good)]
    assert memory.search("calibration",scope="main",limit=5)==[{"id":1,"summary":owned["record"]["summary"]}]


def test_bulk_hydration_preserves_scope_retirement_and_digest_filtering(memory):
    owned=memory.store(scope="main",operation_id="owned",observation=observation("Current calibration"))
    memory.store(scope="other",operation_id="foreign",observation=observation("Private calibration"))
    retired=memory.store(scope="main",operation_id="retired",observation=observation("Old calibration"))
    stale=memory.store(scope="main",operation_id="stale",observation=observation("Stale calibration"))
    with memory._connect() as db:
        db.execute("UPDATE records SET retired=1 WHERE ref=?",(retired["record_ref"],))
        db.execute("UPDATE chunks SET content_digest='changed' WHERE record_ref=?",(stale["record_ref"],))
    assert memory.search("calibration",scope="main",limit=5)==[{"id":1,"summary":owned["record"]["summary"]}]


def test_large_hydration_inputs_respect_sql_variable_bounds(memory):
    class EmptyDB:
        calls=[]
        def execute(self, sql, parameters):
            self.calls.append((sql,parameters))
            assert len(parameters)<=257
            return self
        def fetchall(self):return []
    db=EmptyDB()
    hits=[[{"id":f"chunk-{index}","content_digest":"fixture"} for index in range(512)]]
    assert memory._hydrate_vector_channels(db,hits,"main",False)==[[]]
    assert len(db.calls)==2


def test_vector_hydration_work_does_not_scan_unrelated_originals(memory):
    for index in range(96):
        memory.store(scope="main", operation_id=f"bounded-{index}",
                     observation=observation(f"Optical calibration {index}"))
    hits=[{"id":key,"content_digest":value["content_digest"]}
          for key,value in list(memory.projection.items.items())[:2]]
    hits.extend({"id":f"missing-{index}","content_digest":"absent"} for index in range(254))
    steps=[0]
    def count_step():
        steps[0]+=1
        return 0
    with memory._connect() as db:
        db.set_progress_handler(count_step,1)
        try:
            channels=memory._hydrate_vector_channels(db,[hits],"main",False)
        finally:
            db.set_progress_handler(None,0)
    assert [row["id"] for row in channels[0]]==[1,2]
    assert steps[0]<5000, f"bounded hits scanned unrelated originals: {steps[0]} SQLite VM steps"
