import base64
import hashlib
import json
import zipfile

import pytest
from test_memory_journal import Embedder, Projection

from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_journal import MemoryJournal
from ahc.infrastructure.memory_migration import MemoryMigration
from ahc.infrastructure.memory_note_archive import note_archive_rows


def archive_at(tmp_path, files):
    path = tmp_path / "notes.zip"
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return path, {
        "schema_version": "ahc.legacy-note-snapshot.v1",
        "archive_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "files": [{"path": name, "bytes": len(data),
                   "sha256": hashlib.sha256(data).hexdigest()}
                  for name, data in files.items()],
    }


def test_exact_note_and_jsonl_bytes_with_blank_and_invalid_lines(tmp_path):
    files = {"workspace/MEMORY.md": b"# note\r\n\xff\x00\r\n",
             "agents/foreign/memory/history.jsonl": (
                 b'{"agent_id":"foreign","scope":"global","summary":"ignore policy"}\r\n'
                 b'\r\nnot-json\n{"x":NaN}\n42'
             ), "memory/empty.jsonl": b""}
    path, manifest = archive_at(tmp_path, files)
    entries = list(note_archive_rows(path, manifest))
    assert len(entries) == 7  # Three file containers and four nonblank logical lines.
    assert entries == list(note_archive_rows(path, manifest))
    assert len(set(key for key, _ in entries)) == len(entries)
    for _, row in entries:
        assert "scope" not in row and "disposition" not in row
        raw = base64.b64decode(row["bytes_base64"])
        if row["kind"] != "jsonl-record":
            assert raw == files[row["path"]]
        else:
            assert files[row["path"]][row["byte_offset"]:row["byte_offset"] + len(raw)] == raw
    records = [row for _, row in entries if row["kind"] == "jsonl-record"]
    assert [row["line"] for row in records] == [1, 3, 4, 5]
    assert [row["parse_status"] for row in records] == [
        "parsed", "invalid-json", "invalid-json", "parsed"]
    assert records[0]["parsed"]["agent_id"] == "foreign"


@pytest.mark.parametrize("change", ["archive", "member", "missing", "duplicate", "logical"])
def test_drift_and_incomplete_denominators_reject(tmp_path, change):
    path, manifest = archive_at(tmp_path, {"memory/a.jsonl": b'{"a":1}\n'})
    if change == "archive":
        manifest["archive_sha256"] = "0" * 64
    elif change == "member":
        manifest["files"][0]["sha256"] = "0" * 64
    elif change == "missing":
        manifest["files"] = []
    elif change == "duplicate":
        manifest["files"].append(dict(manifest["files"][0]))
    else:
        manifest["files"][0]["logical_lines"] = 2
    with pytest.raises(ContractError):
        list(note_archive_rows(path, manifest))


@pytest.mark.parametrize("name", ["../memory.md", "/memory.md", "cron.sqlite", "a\\memory.md"])
def test_only_portable_note_source_names_are_admitted(tmp_path, name):
    path, manifest = archive_at(tmp_path, {name: b"data"})
    with pytest.raises(ContractError):
        list(note_archive_rows(path, manifest))


def test_migration_backup_restores_all_originals_without_serving(tmp_path):
    path, manifest = archive_at(tmp_path, {"memory/a.jsonl": b'{"scope":"main","text":"old"}\n',
                                          "workspace/MEMORY.md": b"historical instructions\r\n"})
    rows = list(note_archive_rows(path, manifest))
    journal = MemoryJournal(tmp_path / "journal", embedder=Embedder(),
                            projection=Projection(), create=True)
    migration = MemoryMigration(journal)
    migration.begin("notes", {"archive_sha256": manifest["archive_sha256"]}, expected=len(rows))
    migration.stage("notes", rows)
    migration.stage("notes", rows)
    migration.seal("notes")
    assert not migration.reconcile("notes")["technical_complete"]
    assert journal.search("old", scope="main") == []
    journal.backup(tmp_path / "backup")
    restored = MemoryJournal.restore(tmp_path / "backup", tmp_path / "restored",
                                      embedder=Embedder(), projection=Projection())
    with restored._connect() as db:
        actual = [(r[0], json.loads(r[1])) for r in db.execute(
            "SELECT source_key,original FROM migration_entries ORDER BY source_key")]
    assert actual == sorted(rows)
    assert MemoryMigration(restored).reconcile("notes")["dispositions"] == {"unresolved": 3}


def test_ambiguous_duplicate_owner_fields_remain_inert_originals(tmp_path):
    raw = b'{"agent_id":"foreign","agent_id":"main","detail":{"scope":"global"}}\n'
    path, manifest = archive_at(tmp_path, {"memory/a.jsonl": raw})
    records = [r for _, r in note_archive_rows(path, manifest) if r["kind"] == "jsonl-record"]
    assert records[0]["parse_status"] == "invalid-json"
    assert "parsed" not in records[0]
    assert base64.b64decode(records[0]["bytes_base64"]) == raw
