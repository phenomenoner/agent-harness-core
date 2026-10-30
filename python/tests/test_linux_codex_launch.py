import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ahc.adapters import linux_codex_launch as launch


@pytest.mark.parametrize("path", ["/", "/a/../b", "/a:other", "/a b", "/a\\b", "//a", "relative"])
def test_systemd_path_metacharacters_rejected(path):
    with pytest.raises(launch.LaunchRejected):
        launch._path(path)


def fixture(tmp_path, monkeypatch):
    paths = {key: tmp_path / key for key in ("bundle", "policy_source", "home", "workspace")}
    paths["home"].mkdir()
    data = (
        "[permissions.offline.filesystem]\n" + json.dumps(str(paths["workspace"])) + ' = "write"\n'
        "[permissions.offline.network]\nenabled = false\n"
    ).encode()
    paths["policy_source"].write_bytes(data)
    spec = {key: key for key in paths}
    spec.update(
        profile="offline", policy_sha256=hashlib.sha256(data).hexdigest(), home_size_mib=128
    )
    monkeypatch.setattr(launch.sys, "platform", "linux")
    monkeypatch.setattr(launch.os, "geteuid", lambda: 0, raising=False)
    monkeypatch.setattr(
        launch, "_path", lambda value: paths[value] if value in paths else Path(value)
    )
    monkeypatch.setattr(launch, "_protected", lambda *a, **kw: None)
    calls = []

    def verify(path, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(
            root=path,
            target=kwargs["target"],
            permissions_checked=True,
            root_ownership_checked=True,
        )

    return paths, spec, calls, verify


def test_offline_profile_requests_protected_verification(tmp_path, monkeypatch):
    paths, spec, calls, verify = fixture(tmp_path, monkeypatch)
    value = launch.prepare(spec, verify)
    assert calls == [{"target": "x86_64-unknown-linux-musl", "require_root_owned": True}]
    assert "PrivateNetwork=yes" in value["properties"]
    assert "CODEX_HOME=" + str(paths["home"]) in value["command"]
    assert value["command"][0:2] == ["/usr/bin/env", "-i"]


@pytest.mark.parametrize("mutation", ["digest", "nonempty", "network", "memory", "unprotected"])
def test_rejects_before_launch_plan(tmp_path, monkeypatch, mutation):
    paths, spec, calls, verify = fixture(tmp_path, monkeypatch)
    if mutation == "digest":
        spec["policy_sha256"] = "0" * 64
    if mutation == "nonempty":
        (paths["home"] / "user-file").write_text("retain")
    if mutation == "network":
        data = paths["policy_source"].read_bytes().replace(b"false", b"0")
        paths["policy_source"].write_bytes(data)
        spec["policy_sha256"] = hashlib.sha256(data).hexdigest()
    if mutation == "memory":
        spec["home_size_mib"] = True
    if mutation == "unprotected":

        def verify(path, **kwargs):
            return SimpleNamespace(
                root=path,
                target=kwargs["target"],
                permissions_checked=True,
                root_ownership_checked=False,
            )

    with pytest.raises(launch.LaunchRejected):
        launch.prepare(spec, verify)


def test_protected_arguments_bind_exact_command_and_working_directory(tmp_path, monkeypatch):
    paths, spec, calls, verify = fixture(tmp_path, monkeypatch)
    plan = launch.prepare(spec, verify)
    arguments = ["/usr/bin/systemd-run", "--uid=ahc", "--", *plan["command"]]
    result = launch.protect_arguments(arguments, spec, verify)
    assert result[result.index("--") + 1 :] == plan["command"]
    assert "--property=PrivateNetwork=yes" in result
    assert "--working-directory=" + str(paths["workspace"]) in result
    assert arguments == ["/usr/bin/systemd-run", "--uid=ahc", "--", *plan["command"]]
    with pytest.raises(launch.LaunchRejected, match="differs"):
        launch.protect_arguments(["/usr/bin/systemd-run", "--", "/bin/sh"], spec, verify)


@pytest.mark.parametrize("data", [b"permissions=1", b"[permissions]\noffline=1"])
def test_malformed_profile_has_normalized_rejection(tmp_path, monkeypatch, data):
    paths, spec, calls, verify = fixture(tmp_path, monkeypatch)
    paths["policy_source"].write_bytes(data)
    spec["policy_sha256"] = hashlib.sha256(data).hexdigest()
    with pytest.raises(launch.LaunchRejected):
        launch.prepare(spec, verify)
    assert calls == []
