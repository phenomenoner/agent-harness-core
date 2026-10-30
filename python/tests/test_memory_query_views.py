"""Scenario evidence for optional query views, independent of frozen subjects."""
import pytest
from test_memory_journal import memory, observation

QUERY = "How does Mira evaluate lens alignment under thermal stress?"


def route(memory, *, secondary_failure=False, poison=False):
    distractors=[]
    for i in range(8):
        distractors.append(memory.store(scope="main",operation_id=f"scene-{i}",
            observation=observation(f"Mira signs fictional festival receipt {i}")))
    target=memory.store(scope="main",operation_id="optics",
        observation=observation("Camera optics are inspected using a collimated beam"))
    foreign=memory.store(scope="other",operation_id="foreign",
        observation=observation("Private optical secret from another owner"))
    by_ref={k.rsplit(":",1)[0]: {"id":k,"content_digest":v["content_digest"]}
            for k,v in memory.projection.items.items()}
    exact=[by_ref[r["record_ref"]] for r in distractors]
    target_hit=by_ref[target["record_ref"]]
    foreign_hit=by_ref[foreign["record_ref"]]
    memory.embedder.embed=lambda texts:[[1.0,0.0] if "Mira" in text else [0.0,1.0] for text in texts]
    def query(*,scope,vector,limit):
        if vector[0]:return exact
        if secondary_failure:raise TimeoutError("optional view unavailable")
        if poison:return [foreign_hit,{**exact[0],"content_digest":"wrong"},target_hit]
        return [target_hit]
    memory.projection.query=query
    return target,foreign,distractors


def test_named_actor_does_not_hide_semantic_topic_in_search_or_pack(memory):
    target,_,_=route(memory)
    rows=memory.search(QUERY,scope="main",limit=5)
    assert any("collimated beam" in r["summary"] for r in rows)
    pack=memory.pack(query=QUERY,scope="main",limit=5,budget_tokens=8000)
    assert target["record_ref"] in [i["recordRef"] for i in pack["context_pack"]["items"]]


def test_optional_view_failure_keeps_original_success(memory):
    _,_,distractors=route(memory,secondary_failure=True)
    rows=memory.search(QUERY,scope="main",limit=5)
    assert rows and rows[0]["summary"]==distractors[0]["record"]["summary"]


def test_new_candidates_still_require_exact_scope_and_digest(memory):
    target,foreign,_=route(memory,poison=True)
    rows=memory.search(QUERY,scope="main",limit=5)
    assert any("collimated beam" in r["summary"] for r in rows)
    assert all("Private optical secret" not in r["summary"] for r in rows)
    assert memory.get(scope="main",record_ref=foreign["record_ref"])["status"]!="succeeded"


def test_entity_bearing_original_channel_remains_first(memory):
    target,_,_=route(memory)
    memory.embedder.embed=lambda texts:[[0.0,1.0] if "Mira" in t else [1.0,0.0] for t in texts]
    rows=memory.search(QUERY,scope="main",limit=5)
    assert "collimated beam" in rows[0]["summary"]
