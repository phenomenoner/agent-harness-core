"""Offline, digest-pinned multilingual vectors; no download or cloud fallback."""

import hashlib
import threading
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json

MODEL_REVISION = "5090578d9565bb06545b4552f76e6bc2c93e4a66"
MODEL_FILES = {
    "onnx/model_quantized.onnx": "172efde319fe1542dc41f31be6154910b05b78f7a861c265c4600eec906bd6d8",
    "onnx/model_quantized.onnx_data": (
        "705626e28e4c23c82ade34566b4197d97f534c12275fa406dfb71e9937d388c0"
    ),
    "tokenizer.json": "4dda02faaf32bc91031dc8c88457ac272b00c1016cc679757d1c441b248b9c47",
}
MODEL_IDENTITY = digest_json(
    {
        "repository": "onnx-community/embeddinggemma-300m-ONNX",
        "revision": MODEL_REVISION,
        "files": MODEL_FILES,
        "dimensions": 384,
        "prefix": "task: sentence similarity | query: ",
        "max_tokens": 2048,
    }
)


class LocalEmbedding:
    """Explicit CPU ONNX inference using the pinned MemPalace model convention."""

    identity = MODEL_IDENTITY

    def __init__(self, model_root, *, threads=2):
        import numpy as np
        import onnxruntime as ort
        from tokenizers import Tokenizer

        root = Path(model_root)
        if type(threads) is not int or not 1 <= threads <= 8:
            raise ContractError("embedding thread bound invalid")
        for name, expected in MODEL_FILES.items():
            path = root / name
            if path.is_symlink() or not path.is_file():
                raise ContractError("pinned embedding files unavailable")
            with path.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
                    raise ContractError("embedding artifact digest mismatch")
        options = ort.SessionOptions()
        options.intra_op_num_threads = threads
        options.inter_op_num_threads = 1
        self.session = ort.InferenceSession(
            str(root / "onnx/model_quantized.onnx"),
            sess_options=options,
            providers=["CPUExecutionProvider"],
        )
        self.tokenizer = Tokenizer.from_file(str(root / "tokenizer.json"))
        self.tokenizer.enable_padding()
        self.tokenizer.no_truncation()
        self.np = np
        self.lock = threading.Lock()
        if "sentence_embedding" not in {out.name for out in self.session.get_outputs()}:
            raise ContractError("embedding output contract changed")

    def split_text(self, text):
        """Keep every source character while respecting the model's token limit."""
        if type(text) is not str or not text or len(text) > 131072:
            raise ContractError("embedding document bounds invalid")
        result = []
        pending = [text[i : i + 1500] for i in range(0, len(text), 1500)][::-1]
        with self.lock:
            while pending:
                part = pending.pop()
                encoded = self.tokenizer.encode("task: sentence similarity | query: " + part)
                if len(encoded.ids) <= 2048:
                    result.append(part)
                elif len(part) > 1:
                    middle = len(part) // 2
                    pending.extend((part[middle:], part[:middle]))
                else:
                    raise ContractError("single character exceeds embedding token limit")
        return result

    def embed(self, texts):
        if (
            type(texts) is not list
            or len(texts) > 32
            or any(type(text) is not str or not text or len(text) > 8192 for text in texts)
        ):
            raise ContractError("embedding input bounds invalid")
        result = []
        np = self.np
        # One item at a time bounds attention memory even for long multilingual text.
        with self.lock:
            for text in texts:
                encoded = self.tokenizer.encode("task: sentence similarity | query: " + text)
                if len(encoded.ids) > 2048:
                    raise ContractError("embedding token limit exceeded; split document first")
                ids = np.asarray([encoded.ids], dtype=np.int64)
                vocab = self.tokenizer.get_vocab_size(with_added_tokens=False)
                bad = (ids < 0) | (ids >= vocab)
                if np.any(bad):
                    unknown = self.tokenizer.token_to_id("<unk>")
                    if unknown is None or not 0 <= unknown < vocab:
                        raise ContractError("embedding token outside vocabulary")
                    ids[bad] = unknown
                vector = self.session.run(
                    ["sentence_embedding"],
                    {
                        "input_ids": ids,
                        "attention_mask": np.asarray([encoded.attention_mask], dtype=np.int64),
                    },
                )[0][0, :384]
                norm = float(np.linalg.norm(vector))
                if not np.isfinite(vector).all() or not np.isfinite(norm) or norm <= 0:
                    raise ContractError("local embedding produced invalid vector")
                result.append((vector / norm).astype(float).tolist())
        return result
