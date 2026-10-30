"""Chunk coverage and token limits are custody contracts, not quality scores."""

import threading
from types import SimpleNamespace

import pytest

from ahc.adapters.local_embedding import LocalEmbedding
from ahc.domain.sessions import ContractError


class ByteTokenizer:
    def encode(self, text):
        return SimpleNamespace(ids=list(text.encode()) + [1, 2])


def embedder():
    value = LocalEmbedding.__new__(LocalEmbedding)
    value.tokenizer = ByteTokenizer()
    value.lock = threading.Lock()
    value.np = None
    return value


def test_multibyte_document_chunks_cover_every_character_within_token_limit():
    value = embedder()
    text = "🧬漢字 and code " * 500 + "TRAILING-EVIDENCE"
    chunks = value.split_text(text)
    assert "".join(chunks) == text
    assert len(chunks) > 1
    assert all(
        len(value.tokenizer.encode("task: sentence similarity | query: " + p).ids) <= 2048
        for p in chunks
    )
    assert chunks[-1].endswith("TRAILING-EVIDENCE")


def test_embedding_overflow_is_rejected_before_inference_instead_of_truncated():
    with pytest.raises(ContractError, match="token limit exceeded"):
        embedder().embed(["🧬" * 1500])


@pytest.mark.parametrize("value", ["", None, "x" * 131073], ids=("empty", "wrong-type", "too-long"))
def test_document_size_is_bounded(value):
    with pytest.raises(ContractError):
        embedder().split_text(value)
