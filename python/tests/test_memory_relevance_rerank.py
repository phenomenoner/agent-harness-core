import json
import pytest
from ahc.domain.sessions import ContractError
from ahc.adapters.local_reranker import LocalReranker
from test_memory_journal import memory, observation

class Scorer:
 def __init__(self,call):self.call=call
 def rerank(self,query,items):return self.call(items)

def owned(memory):
 a=memory.store(scope="main",operation_id="first",observation=observation("Thermometer calibration schedule"))
 b=memory.store(scope="main",operation_id="second",observation=observation("Spectrometer wavelength calibration"))
 memory.store(scope="foreign",operation_id="third",observation=observation("Private spectrometer calibration"))
 return a,b

def test_reranking_uses_only_post_inference_canonical_text(memory):
 owned(memory)
 def spoof(items):
  for item in items:item["summary"]="forged scorer text"
  return list(reversed(items))
 memory.reranker=Scorer(spoof)
 assert memory.search("calibration",scope="main",limit=2)==[{"id":2,"summary":"Spectrometer wavelength calibration"},{"id":1,"summary":"Thermometer calibration schedule"}]

def test_foreign_and_duplicate_scorer_locators_cannot_expand_scope(memory):
 owned(memory)
 memory.reranker=Scorer(lambda items:[{"id":3,"summary":"foreign"}]+items)
 rows=memory.search("calibration",scope="main",limit=5)
 assert {r["id"] for r in rows}=={1,2}
 assert all("Private" not in r["summary"] for r in rows)

@pytest.mark.parametrize("fails",[False,True])
def test_retirement_during_inference_is_rechecked_even_on_fallback(memory,fails):
 a,b=owned(memory)
 def retire(items):
  memory.revise(scope="main",operation_id="retire-during-score",expected_ref=a["record_ref"])
  if fails:raise TimeoutError("optional inference")
  return items
 memory.reranker=Scorer(retire)
 assert memory.search("calibration",scope="main",limit=5)==[{"id":2,"summary":"Spectrometer wavelength calibration"}]

def test_digest_tamper_during_inference_is_not_disclosed(memory):
 owned(memory)
 def tamper(items):
  with memory._connect() as db:
   row=db.execute("SELECT observation FROM records WHERE id=1").fetchone();value=json.loads(row[0]);value["summary"]="tampered"
   db.execute("UPDATE records SET observation=? WHERE id=1",(json.dumps(value),))
  return items
 memory.reranker=Scorer(tamper)
 with pytest.raises(ContractError):memory.search("calibration",scope="main",limit=5)

def test_wrong_model_hash_is_rejected_before_inference(tmp_path):
 root=tmp_path/"model";(root/"onnx").mkdir(parents=True);(root/"onnx/model_quint8_avx2.onnx").write_bytes(b"wrong")
 with pytest.raises(ValueError,match="artifact mismatch"):LocalReranker(root).rerank("query",[{"id":1,"summary":"one"},{"id":2,"summary":"two"}])
