"""Read-only cgroup-v2 effective resource observation."""

from pathlib import Path

import pytest

from ahc.adapters import effective_resources


_DEFAULT_LIMITS = {
    "memory.max": "max\n",
    "pids.max": "max\n",
    "cpu.max": "max 100000\n",
}


def _write_stat(proc_root: Path, pid: int, starttime: int = 1234) -> None:
    process = proc_root / str(pid)
    process.mkdir(parents=True, exist_ok=True)
    # A closing paren inside comm catches parsers that split at the first ')'.
    tail = ["S", *("1" for _ in range(18)), str(starttime), "0", "0"]
    (process / "stat").write_text(
        f"{pid} (worker ) with spaces) {' '.join(tail)}\n", encoding="ascii"
    )


def _write_hierarchy(root: Path, cgroup_path: str, overrides=None) -> None:
    overrides = overrides or {}
    parts = () if cgroup_path == "/" else tuple(cgroup_path[1:].split("/"))
    for depth in range(len(parts) + 1):
        path = "/" + "/".join(parts[:depth]) if depth else "/"
        directory = root.joinpath(*parts[:depth])
        directory.mkdir(parents=True, exist_ok=True)
        values = {**_DEFAULT_LIMITS, **overrides.get(path, {})}
        for name, value in values.items():
            (directory / name).write_text(value, encoding="ascii")


def _fixture(tmp_path: Path, *, pid: int = 4321, cgroup_path: str = "/team/worker"):
    proc_root = tmp_path / "proc"
    cgroup_root = tmp_path / "cgroup"
    _write_stat(proc_root, pid)
    (proc_root / str(pid) / "cgroup").write_text(
        f"0::{cgroup_path}\n", encoding="ascii"
    )
    _write_hierarchy(cgroup_root, cgroup_path)
    return pid, proc_root, cgroup_root


def test_effective_memory_and_pids_use_finite_ancestor_when_leaf_is_unlimited(tmp_path):
    pid, proc_root, cgroup_root = _fixture(
        tmp_path,
        cgroup_path="/team/worker",
    )
    _write_hierarchy(
        cgroup_root,
        "/team/worker",
        {
            "/": {"memory.max": "8000", "pids.max": "80"},
            "/team": {"memory.max": "4000", "pids.max": "40"},
            "/team/worker": {"memory.max": "max", "pids.max": "max"},
        },
    )

    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )

    assert result["identity_verified"] is True
    assert result["status"] == "observed"
    assert result["cgroup_path"] == "/team/worker"
    assert result["leaf"]["path"] == "/team/worker"
    assert [item["path"] for item in result["ancestors"]] == ["/team", "/"]
    assert result["effective_limits"]["memory"] == {
        "state": "finite",
        "value": "4000",
        "source": "/team",
    }
    assert result["effective_limits"]["pids"] == {
        "state": "finite",
        "value": "40",
        "source": "/team",
    }


def test_cpu_uses_exact_ratio_and_reports_the_limiting_ancestor(tmp_path):
    pid, proc_root, cgroup_root = _fixture(tmp_path)
    _write_hierarchy(
        cgroup_root,
        "/team/worker",
        {
            "/": {"cpu.max": "max 100000"},
            "/team": {"cpu.max": "45000 100000"},
            "/team/worker": {"cpu.max": "70000 100000"},
        },
    )

    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )

    assert result["effective_limits"]["cpu"] == {
        "state": "finite",
        "quota": "45000",
        "period": "100000",
        "ratio": "9/20",
        "source": "/team",
    }


def test_missing_limit_stays_unknown_instead_of_becoming_unlimited(tmp_path):
    pid, proc_root, cgroup_root = _fixture(tmp_path)
    (cgroup_root / "team" / "worker" / "memory.max").unlink()

    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )

    assert result["status"] == "partial"
    assert result["leaf"]["controllers"]["memory"] == {
        "state": "unknown",
        "reason": "unavailable",
    }
    assert result["effective_limits"]["memory"] == {
        "state": "unknown",
        "reason": "ancestor-unavailable",
    }


def test_unreadable_limit_stays_unknown(tmp_path, monkeypatch):
    pid, proc_root, cgroup_root = _fixture(tmp_path)
    read_limit = effective_resources._read_limit_file

    def deny_pids(directory_fd, name):
        if name == "pids.max":
            raise PermissionError("fixture unreadable")
        return read_limit(directory_fd, name)

    monkeypatch.setattr(effective_resources, "_read_limit_file", deny_pids)
    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )

    assert result["status"] == "partial"
    assert result["leaf"]["controllers"]["pids"] == {
        "state": "unknown",
        "reason": "unavailable",
    }
    assert result["effective_limits"]["pids"]["state"] == "unknown"


def test_malformed_limit_stays_unknown(tmp_path):
    pid, proc_root, cgroup_root = _fixture(tmp_path)
    (cgroup_root / "team" / "worker" / "cpu.max").write_text(
        "not-a-quota 100000\n", encoding="ascii"
    )

    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )

    assert result["status"] == "partial"
    assert result["leaf"]["controllers"]["cpu"] == {
        "state": "unknown",
        "reason": "invalid",
    }
    assert result["effective_limits"]["cpu"]["state"] == "unknown"


@pytest.mark.parametrize("path", ["/../outside", "//outside", "/team/../../outside"])
def test_traversal_or_absolute_prefix_escape_is_rejected(tmp_path, path):
    pid, proc_root, cgroup_root = _fixture(tmp_path)
    (proc_root / str(pid) / "cgroup").write_text(f"0::{path}\n", encoding="ascii")
    outside = tmp_path / "outside"
    _write_hierarchy(outside, "/")

    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )

    assert result["status"] == "unavailable"
    assert result["identity_verified"] is True
    assert result["leaf"] is None
    assert all(value["state"] == "unknown"
               for value in result["effective_limits"].values())


def test_symlink_component_cannot_escape_cgroup_root(tmp_path):
    pid, proc_root, cgroup_root = _fixture(tmp_path, cgroup_path="/escape")
    outside = tmp_path / "outside"
    _write_hierarchy(outside, "/")
    for item in (cgroup_root / "escape").iterdir():
        item.unlink()
    (cgroup_root / "escape").rmdir()
    (cgroup_root / "escape").symlink_to(outside, target_is_directory=True)

    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )

    assert result["status"] == "unavailable"
    assert result["leaf"] is None
    assert result["effective_limits"]["memory"]["state"] == "unknown"


def test_depth_limit_returns_partial_and_does_not_claim_effective_limits(tmp_path):
    pid, proc_root, cgroup_root = _fixture(tmp_path)

    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root, max_depth=2
    )

    assert result["status"] == "partial"
    assert result["hierarchy_complete"] is False
    assert len(result["ancestors"]) == 1
    assert result["effective_limits"]["memory"] == {
        "state": "unknown",
        "reason": "depth-limit",
    }


def test_duplicate_v2_membership_is_unavailable(tmp_path):
    pid, proc_root, cgroup_root = _fixture(tmp_path)
    (proc_root / str(pid) / "cgroup").write_text(
        "0::/team/worker\n0::/other\n", encoding="ascii"
    )

    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )

    assert result["status"] == "unavailable"
    assert result["reason"] == "missing-or-duplicate-v2-entry"


def test_pid_reuse_discards_observations(monkeypatch, tmp_path):
    pid, proc_root, cgroup_root = _fixture(tmp_path)
    values = iter(((1234, None), (1235, None)))
    monkeypatch.setattr(
        effective_resources,
        "_read_process_starttime",
        lambda _pid, _root: next(values),
    )

    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )

    assert result["status"] == "unavailable"
    assert result["identity_verified"] is False
    assert result["leaf"] is None


def test_absent_process_is_unavailable(tmp_path):
    result = effective_resources.observe_process_resources(
        9876, proc_root=tmp_path / "proc", cgroup_root=tmp_path / "cgroup"
    )

    assert result["status"] == "unavailable"
    assert result["identity_verified"] is False
    assert result["reason"] == "process-unavailable"


@pytest.mark.parametrize("pid", [True, 0, -1, "12"])
def test_pid_must_be_positive_exact_int(pid):
    with pytest.raises(ValueError, match="pid"):
        effective_resources.observe_process_resources(pid)


@pytest.mark.parametrize("max_depth", [True, 0, 129, "32"])
def test_max_depth_is_bounded_exact_int(max_depth):
    with pytest.raises(ValueError, match="max_depth"):
        effective_resources.observe_process_resources(1, max_depth=max_depth)


def test_root_cgroup_has_no_max_files_and_does_not_hide_finite_child_limit(tmp_path):
    pid, proc_root, cgroup_root = _fixture(tmp_path)
    for filename in _DEFAULT_LIMITS:
        (cgroup_root / filename).unlink()
    (cgroup_root / "team" / "pids.max").write_text("40\n")
    result = effective_resources.observe_process_resources(
        pid, proc_root=proc_root, cgroup_root=cgroup_root
    )
    assert result["status"] == "observed"
    assert result["effective_limits"]["pids"] == {
        "state": "finite", "value": "40", "source": "/team"
    }
    assert result["effective_limits"]["memory"]["state"] == "unlimited"
    assert result["ancestors"][-1]["controllers"]["pids"]["state"] == "not-applicable"
