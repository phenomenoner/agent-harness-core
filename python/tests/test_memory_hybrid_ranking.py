"""Ranking regressions on held-out subjects, not the frozen historical benchmark."""
import pytest
from test_memory_journal import memory, observation
from test_memory_keyword_fallback import build


def test_complete_corpus_fuses_lexical_target_outside_old_vector_window(memory):
    for i in range(39):
        memory.store(scope="main", operation_id=f"noise-{i}",
                     observation=observation(f"Unrelated discussion number {i}"))
    target = memory.store(scope="main", operation_id="target",
                          observation=observation("A spectrometer requires wavelength calibration"))
    build_large(memory, "main")
    requested = []
    original = memory.projection.query
    def query(**kwargs):
        requested.append(kwargs["limit"])
        return original(**kwargs)
    memory.projection.query = query
    rows = memory.search("How is spectrometer wavelength calibration performed?", scope="main", limit=5)
    assert 40 in [row["id"] for row in rows]
    assert requested == [256]
    assert memory.get(scope="main", record_ref=target["record_ref"])["status"] == "succeeded"


def build_large(memory, scope):
    for _ in range(3):
        if not memory.build_keyword_index(scope=scope, limit=128):
            return
    raise AssertionError("bounded fixture build")


@pytest.mark.parametrize("summary,query", [
    ("機房設備故障需先檢查散熱與電源", "請找出機房設備故障應如何處理的紀錄"),
    ("山區救援隊的裝備清單與輪值安排", "山區救援隊應準備哪些裝備呢"),
    ("晶片製造需要穩定的無塵室環境", "晶片製造過程有哪些環境條件"),
])
def test_cjk_question_fragments_retrieve_without_embedding(memory, summary, query):
    memory.projection.fail = True
    memory.store(scope="main", operation_id="owned", observation=observation(summary))
    memory.store(scope="other", operation_id="foreign", observation=observation(summary + " secret"))
    build_large(memory, "main")
    memory.embedder.embed = lambda *_: (_ for _ in ()).throw(AssertionError("keyword read"))
    assert memory.search(query, scope="main") == [{"id": 1, "summary": summary}]


def test_fusion_does_not_admit_foreign_stale_or_retired_vector_rows(memory):
    owned = memory.store(scope="main", operation_id="owned", observation=observation("spectrometer calibration"))
    memory.store(scope="other", operation_id="foreign", observation=observation("spectrometer calibration secret"))
    retired = memory.store(scope="main", operation_id="retired", observation=observation("spectrometer calibration old"))
    memory.revise(scope="main", operation_id="retire", expected_ref=retired["record_ref"])
    build_large(memory, "main")
    original = memory.projection.query
    stale = {"id": next(iter(memory.projection.items)), "content_digest": "wrong"}
    memory.projection.query = lambda **kwargs: [stale] + original(**kwargs)
    rows = memory.search("spectrometer calibration", scope="main", limit=5)
    assert rows == [{"id": 1, "summary": owned["record"]["summary"]}]


def test_query_fragments_are_bounded_and_fts_operators_remain_inert():
    from ahc.infrastructure.memory_keyword_search import _query_terms
    terms = _query_terms("光" * 5000 + ' \" OR NEAR*')
    assert len(terms) <= 48
    assert all(3 <= len(t) <= 32 for t in terms)
    assert _query_terms("甲乙 a 42") == []


@pytest.mark.parametrize("query", ["WHO", "who", "Python安裝步驟", "安裝步驟Python"])
def test_acronyms_and_mixed_script_questions_survive_keyword_fallback(memory, query):
    memory.projection.fail = True
    summary = "WHO vaccination guidance; Python 的安裝步驟"
    memory.store(scope="main", operation_id="held-out", observation=observation(summary))
    build_large(memory, "main")
    memory.embedder.embed = lambda *_: (_ for _ in ()).throw(AssertionError("keyword read"))
    assert memory.search(query, scope="main") == [{"id": 1, "summary": summary}]


def test_single_channel_exact_head_survives_many_broad_agreements(memory):
    for i in range(39):
        memory.store(scope="main", operation_id=f"broad-{i}",
                     observation=observation(f"Spectrometer equipment discussion {i}"))
    memory.store(scope="main", operation_id="precise",
                 observation=observation("Spectrometer wavelength calibration reference"))
    build_large(memory, "main")
    original = memory.projection.query
    memory.projection.query = lambda **kw: [r for r in original(**kw) if not r["id"].endswith(":never")][:-1]
    rows = memory.search("spectrometer wavelength calibration", scope="main", limit=5)
    assert 40 in [row["id"] for row in rows]


def test_plural_query_preserves_keyword_recall_when_vectors_are_missing(memory):
    memory.projection.fail = True
    summary = "Spectrometer wavelength calibration reference"
    memory.store(scope="main", operation_id="singular", observation=observation(summary))
    build_large(memory, "main")
    memory.embedder.embed = lambda *_: (_ for _ in ()).throw(AssertionError("keyword read"))
    assert memory.search("spectrometers wavelengths calibrations", scope="main") == [{"id": 1, "summary": summary}]


def test_underscore_separates_terms_in_keyword_questions(memory):
    memory.projection.fail = True
    summary = "Python install instructions"
    memory.store(scope="main", operation_id="install", observation=observation(summary))
    build_large(memory, "main")
    assert memory.search("python_install", scope="main") == [{"id": 1, "summary": summary}]


def test_inflection_expansion_keeps_literal_plural_identifiers(memory):
    memory.projection.fail = True
    summary = "Libraries dependency policy"
    memory.store(scope="main", operation_id="libraries", observation=observation(summary))
    build_large(memory, "main")
    assert memory.search("libraries", scope="main")[0]["summary"] == summary
