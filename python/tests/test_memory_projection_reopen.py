"""Opening an existing projection must not create or rewrite its identity."""

import sys
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace

import pytest

from ahc.adapters.mempalace_projection import MemPalaceProjection
from ahc.domain.sessions import ContractError


@pytest.fixture
def backend(monkeypatch):
    @dataclass
    class Identity:
        model: str
        dimensions: int

    calls = []
    collection = SimpleNamespace(get_stored_embedder_identity=lambda: Identity("model", 384),
                                 set_embedder_identity=lambda value: calls.append(("set", value)))

    class Backend:
        def get_collection(self, **options):
            calls.append(("open", options))
            return collection

        def close(self):
            calls.append(("close", None))

    base = ModuleType("mempalace.backends.base")
    base.EmbedderIdentity, base.PalaceRef = Identity, SimpleNamespace
    qdrant = ModuleType("mempalace.backends.qdrant")
    qdrant.QdrantBackend = Backend
    monkeypatch.setitem(sys.modules, "mempalace.backends.base", base)
    monkeypatch.setitem(sys.modules, "mempalace.backends.qdrant", qdrant)
    return collection, calls


def open_projection(**options):
    return MemPalaceProjection(palace_root="/fixture", palace_id="fixture",
        url="http://127.0.0.1:18765", api_key="synthetic-fixture",
        model_identity="model", **options)


def test_existing_open_is_explicit_and_does_not_write_metadata(backend):
    collection, calls = backend
    target = open_projection(create=False)
    assert target.collection is collection
    assert calls[0][1]["create"] is False
    assert len(calls) == 1
    target.close()
    assert calls[-1][0] == "close"


def test_initial_creation_keeps_existing_default(backend):
    _, calls = backend
    target = open_projection()
    assert calls[0][1]["create"] is True
    assert calls[1][0] == "set"
    target.close()


@pytest.mark.parametrize("identity", [None, "foreign"])
def test_existing_open_refuses_missing_or_foreign_identity_without_writes(backend, identity):
    collection, calls = backend
    collection.get_stored_embedder_identity = lambda: identity
    with pytest.raises(ContractError, match="identity absent or changed"):
        open_projection(create=False)
    assert [name for name, _ in calls] == ["open", "close"]


def test_creation_mode_requires_an_actual_boolean_before_open(backend):
    _, calls = backend
    with pytest.raises(ContractError):
        open_projection(create=0)
    assert calls == []
