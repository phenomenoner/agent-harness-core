from ahc.infrastructure.memory_journal import MemoryJournal
from ahc.adapters.local_reranker import LocalReranker
import pytest

def test_independent_channel_survives_overlap_before_scoring():
 first=[{"id":i,"summary":f"candidate {i}"} for i in range(100)]
 second=first[:70]+[{"id":200+i,"summary":f"independent candidate {i}"} for i in range(30)]
 assert 200 not in {x["id"] for x in MemoryJournal._rank_channels([first,second],64)}
 result=MemoryJournal._balanced_shortlist([first,second],128)
 assert 200 in {x["id"] for x in result}
 assert len(result)==128 and len({x["id"] for x in result})==128

def test_identical_text_cannot_consume_multiple_scorer_slots():
 channels=[[{"id":1,"summary":" Alpha  beta "},{"id":2,"summary":"second"}],[],[{"id":3,"summary":"alpha beta"},{"id":4,"summary":"fourth"}]]
 result=MemoryJournal._balanced_shortlist(channels,128)
 assert {x["id"] for x in result}=={1,2,4}
 assert MemoryJournal._balanced_shortlist([],128)==[]

def test_scorer_rejects_over128_without_loading_optional_model(tmp_path):
 with pytest.raises(ValueError,match="candidate bound"):
  LocalReranker(tmp_path).rerank("query",[{"id":i,"summary":"text"} for i in range(129)])
