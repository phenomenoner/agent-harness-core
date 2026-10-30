"""Pinned, optional CPU relevance ranking; never edits original/vector state."""
import hashlib
import time
from pathlib import Path

FILES={"onnx/model_quint8_avx2.onnx":"6c2513767fb63d008a4377bef7a7a3555433d9436342bb53e35a3a72ffc52d4b","tokenizer.json":"62c24cdc13d4c9952d63718d6c9fa4c287974249e16b7ade6d5a85e7bbb75626"}
REPOSITORY="cross-encoder/mmarco-mMiniLMv2-L12-H384-v1"
REVISION="1427fd652930e4ba29e8149678df786c240d8825"
class LocalReranker:
 def __init__(self,root,*,threads=4,budget_seconds=12):
  if type(threads) is not int or not 1<=threads<=4:raise ValueError("reranker thread bound")
  if type(budget_seconds) is not int or not 1<=budget_seconds<=12:raise ValueError("reranker time bound")
  self.root=Path(root);self.threads=threads;self.budget_seconds=budget_seconds;self.loaded=None;self.last_metrics={}
 def _load(self):
  if self.loaded is not None:return self.loaded
  import numpy as np
  import onnxruntime as ort
  from tokenizers import Tokenizer
  if self.root.is_symlink():raise ValueError("reranker root link")
  for name,expected in FILES.items():
   p=self.root/name
   if p.is_symlink() or not p.is_file():raise ValueError("reranker artifact unavailable")
   with p.open("rb") as stream:
    if hashlib.file_digest(stream,"sha256").hexdigest()!=expected:raise ValueError("reranker artifact mismatch")
  options=ort.SessionOptions();options.intra_op_num_threads=self.threads;options.inter_op_num_threads=1
  session=ort.InferenceSession(str(self.root/"onnx/model_quint8_avx2.onnx"),sess_options=options,providers=["CPUExecutionProvider"])
  if [x.name for x in session.get_inputs()]!=["input_ids","attention_mask"]:raise ValueError("reranker input contract")
  tok=Tokenizer.from_file(str(self.root/"tokenizer.json"));tok.enable_truncation(max_length=256,strategy="only_second");tok.no_padding()
  self.loaded=(np,session,tok);return self.loaded
 def rerank(self,query,items):
  if type(query) is not str or not query.strip() or len(query)>4096:raise ValueError("reranker query bound")
  if type(items) is not list or len(items)>128:raise ValueError("reranker candidate bound")
  if any(type(x.get("id")) is not int or type(x.get("summary")) is not str or len(x["summary"])>16384 for x in items):raise ValueError("reranker item bound")
  if len(items)<2:return list(items)
  started=time.monotonic();np,session,tok=self._load();load_seconds=time.monotonic()-started
  if time.monotonic()-started>self.budget_seconds:raise TimeoutError("reranker time budget")
  encoded=tok.encode_batch([(query,x["summary"] or " ") for x in items]);order=sorted(range(len(items)),key=lambda i:(len(encoded[i].ids),i));scores={};batch_seconds=[]
  for offset in range(0,len(order),8):
   if time.monotonic()-started>self.budget_seconds:raise TimeoutError("reranker time budget")
   batch=order[offset:offset+8];length=max(len(encoded[i].ids) for i in batch)
   ids=np.asarray([encoded[i].ids+[1]*(length-len(encoded[i].ids)) for i in batch],dtype=np.int64)
   mask=np.asarray([[1]*len(encoded[i].ids)+[0]*(length-len(encoded[i].ids)) for i in batch],dtype=np.int64)
   before=time.monotonic();pred=np.asarray(session.run(None,{"input_ids":ids,"attention_mask":mask})[0]).reshape(-1);batch_seconds.append(time.monotonic()-before)
   if len(pred)!=len(batch) or not np.isfinite(pred).all():raise ValueError("reranker output contract")
   for i,value in zip(batch,pred):scores[i]=float(value)
  elapsed=time.monotonic()-started
  self.last_metrics={"candidates":len(items),"seconds":str(round(elapsed,3)),"load_seconds":str(round(load_seconds,3)),"max_batch_seconds":str(round(max(batch_seconds),3))}
  if elapsed>self.budget_seconds:raise TimeoutError("reranker time budget")
  return [items[i] for i in sorted(range(len(items)),key=lambda i:(-scores[i],i))]
