"""Metadata inspection with hostile representation hooks and exact bindings."""

import json
import subprocess
import sys

import pytest

from ahc.adapters.workspace_kernel import KERNEL
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.workspace import WorkspaceIdentity
from ahc.domain.workspace_inspection import inspection_names, inspection_result


def test_fixed_kernel_inspection_does_not_call_representation_or_descriptors():
    code = """
called = False
class Evil:
    def __repr__(self):
        global called
        called = True
        raise Exception('SENTINEL')
    def __len__(self):
        global called
        called = True
        raise Exception('SENTINEL')
    @property
    def shape(self):
        global called
        called = True
        raise Exception('SENTINEL')
value = Evil()
number = 42
items = [value]
"""
    frames = [
        {"id": 1, "operation": "execute", "code": code},
        {"id": 2, "operation": "inspect", "names": ["value", "number", "items", "absent"]},
        {"id": 3, "operation": "execute", "code": "print(called)"},
    ]
    result = subprocess.run(
        [sys._base_executable, "-I", "-B", "-c", KERNEL],
        input=b"".join(json.dumps(f).encode() + b"\n" for f in frames),
        capture_output=True,
        timeout=5,
    )
    assert result.returncode == 0
    replies = [json.loads(line) for line in result.stdout.splitlines()]
    assert replies[1]["variables"] == [
        {"name": "value", "kind": "opaque", "size": None},
        {"name": "number", "kind": "integer", "size": None},
        {"name": "items", "kind": "list", "size": 1},
        {"name": "absent", "kind": "missing", "size": None},
    ]
    assert replies[2]["output"] == "False\n" and b"SENTINEL" not in result.stdout


@pytest.mark.parametrize(
    "names",
    [("_private",), ("x", "x"), ("x" * 129,), ["x"]],
    ids=["private", "duplicate", "long", "mutable"],
)
def test_invalid_requested_names_rejected(names):
    with pytest.raises(ContractError):
        inspection_names(names)


def test_metadata_cannot_smuggle_values_or_wrong_names():
    identity = WorkspaceIdentity(
        VirtualSessionIdentity.initial("session", "agent", "route"), "work"
    )
    row = {"name": "x", "kind": "opaque", "size": None}
    assert inspection_result(identity, ("x",), [row])["identity"] == identity.to_dict()
    for bad in ({**row, "value": "SENTINEL"}, {**row, "name": "foreign"}, {**row, "size": True}):
        with pytest.raises(ContractError):
            inspection_result(identity, ("x",), [bad])
