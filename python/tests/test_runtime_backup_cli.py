import json

from test_workspace_tools import setup

from ahc.cli import main


def test_cli_backup_inspect_and_non_destructive_errors(tmp_path, capsys):
    store, workspace, _ = setup(tmp_path)
    destination = tmp_path / "saved"
    args = ["runtime-backup", "--runtime-root", str(store.root),
            "--destination", str(destination)]
    try:
        assert main(args) == 0
        created = json.loads(capsys.readouterr().out)
        before = {p.name: p.read_bytes() for p in destination.iterdir()}
        assert main(["runtime-backup-inspect", "--backup-root", str(destination)]) == 0
        inspected = json.loads(capsys.readouterr().out)
        assert created["backup"] == inspected["backup"]
        assert main(["runtime-restore-assess", "--backup-root", str(destination)]) == 0
        assessment = json.loads(capsys.readouterr().out)["assessment"]
        assert assessment["activation_allowed"] is False
        assert assessment["counts"]["workspace_heads"] == 1
        assert assessment["counts"]["pending_turns"] == 0
        assert assessment["counts"]["backend_held"] is None
        assert assessment["sessions"][0]["session"] == workspace.identity.session.to_dict()
        assert "goal" not in str(assessment) and "input_text" not in str(assessment)
        assert inspected["backup"]["restorable_runtime"] is False
        assert {p.name: p.read_bytes() for p in destination.iterdir()} == before
        assert main(args) != 0
        error = json.loads(capsys.readouterr().out)
        assert error["ok"] is False and str(destination) not in str(error)
        assert {p.name: p.read_bytes() for p in destination.iterdir()} == before
        (destination / "manifest.json").write_text("{broken", encoding="utf-8")
        assert main(["runtime-backup-inspect", "--backup-root", str(destination)]) != 0
        assert json.loads(capsys.readouterr().out)["ok"] is False
    finally:
        workspace.close()
