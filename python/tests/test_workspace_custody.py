"""Execute the cleanup branches; malformed create output never grants custody."""

import pytest

from ahc.adapters.docker_workspace import DockerReply, OwnedContainer, WorkspaceUnavailable


@pytest.mark.parametrize(
    "reply",
    [
        DockerReply(0, ""),
        DockerReply(0, "named-container"),
        DockerReply(0, "a" * 63),
        DockerReply(1, "a" * 64),
        DockerReply(0, "a" * 64 + "\nextra"),
    ],
)
def test_invalid_create_never_inspects_or_removes(reply):
    calls = []

    def runner(args):
        calls.append(args)
        return reply

    with pytest.raises(WorkspaceUnavailable):
        OwnedContainer.create(runner, ("python", "-c", "pass"))
    assert len(calls) == 1 and calls[0][0] == "create"


def test_valid_id_cleanup_is_exact_even_when_inspection_fails():
    calls = []
    identifier = "a" * 64

    def runner(args):
        calls.append(args)
        if args[0] == "create":
            return DockerReply(0, identifier + "\n")
        if args[0] == "inspect":
            return DockerReply(0, "[]")
        if args[0] == "rm":
            return DockerReply(0, identifier)
        return DockerReply(0, "")

    with pytest.raises(WorkspaceUnavailable):
        OwnedContainer.create(runner, ("python", "-c", "pass"))
    assert calls[2] == ["rm", "--force", identifier]
    assert calls[3] == [
        "container",
        "ls",
        "--all",
        "--no-trunc",
        "--filter",
        "id=" + identifier,
        "--format",
        "{{.ID}}",
    ]


def test_failed_durable_record_removes_created_id_before_any_attach():
    calls = []

    def runner(args):
        calls.append(args)
        if args[0] == "create":
            return DockerReply(0, "a" * 64)
        if args[0] == "rm":
            return DockerReply(0, "a" * 64)
        return DockerReply(0, "")

    def fail_record(owned):
        assert owned.container_id == "a" * 64
        raise RuntimeError("record failed")

    with pytest.raises(RuntimeError, match="record failed"):
        OwnedContainer.create(runner, ("python", "fixture"), on_created=fail_record)
    assert [args[0] for args in calls] == ["create", "rm", "container"]
    assert calls[1] == ["rm", "--force", "a" * 64]
