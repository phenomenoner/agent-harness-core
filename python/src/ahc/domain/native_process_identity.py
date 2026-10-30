"""Explicit native cgroup identities, distinct from legacy transient services."""

import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from uuid import UUID

from .sessions import ContractError


@dataclass(frozen=True, slots=True)
class NativeProcessIdentity:
    host_id: str
    boot_id: str
    service: str
    invocation_id: str
    cgroup: str
    device: int
    inode: int
    kind: str = "linux-cgroup.v1"

    def __post_init__(self):
        try:
            valid_boot = str(UUID(self.boot_id)) == self.boot_id
        except (ValueError, TypeError, AttributeError):
            valid_boot = False
        if (self.kind != "linux-cgroup.v1" or not valid_boot
                or type(self.host_id) is not str
                or re.fullmatch(r"[0-9a-f]{32}", self.host_id) is None
                or type(self.invocation_id) is not str
                or re.fullmatch(r"[0-9a-f]{32}", self.invocation_id) is None
                or self.invocation_id == "0" * 32
                or type(self.service) is not str
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@:-]{0,200}\.service", self.service) is None
                or type(self.cgroup) is not str
                or not self.cgroup.startswith("/") or self.cgroup.startswith("//")
                or len(self.cgroup) > 4096 or "\\" in self.cgroup
                or any(ord(char) < 32 for char in self.cgroup)
                or str(PurePosixPath(self.cgroup)) != self.cgroup
                or ".." in PurePosixPath(self.cgroup).parts
                or PurePosixPath(self.cgroup).parent.name != self.service
                or re.fullmatch(r"ahc-process-[0-9a-f]{32}", PurePosixPath(self.cgroup).name) is None
                or type(self.device) is not int or self.device < 0
                or type(self.inode) is not int or self.inode <= 0):
            raise ContractError("native process identity invalid")


@dataclass(frozen=True, slots=True)
class NativeCreationAttempt:
    identity: NativeProcessIdentity
    kind: str = "linux-cgroup-attempt.v1"

    def __post_init__(self):
        if type(self.identity) is not NativeProcessIdentity or self.kind != "linux-cgroup-attempt.v1":
            raise ContractError("native creation attempt invalid")


@dataclass(frozen=True, slots=True)
class NativeProcessObservation:
    identity: NativeProcessIdentity
    state: str
    cgroup_empty: bool | None


def identity_from_dict(value):
    from .backend_identity import LinuxUnitIdentity

    if type(value) is not dict:
        raise ContractError("process identity must be an object")
    if value.get("kind") == "linux-cgroup.v1":
        return NativeProcessIdentity(**value)
    return LinuxUnitIdentity(**value)


def custody_key(identity):
    from .backend_identity import LinuxUnitIdentity
    from .skill_values import digest_json

    if type(identity) is LinuxUnitIdentity:
        return digest_json({"distribution": identity.distribution, "unit": identity.unit})
    if type(identity) is NativeProcessIdentity:
        return digest_json({"kind": identity.kind, "host_id": identity.host_id,
                            "service": identity.service, "invocation_id": identity.invocation_id,
                            "cgroup": identity.cgroup})
    raise ContractError("unsupported process custody identity")


def attempt_from_dict(value):
    from .backend_creation import BackendCreationAttempt

    if type(value) is dict and value.get("kind") == "linux-cgroup-attempt.v1":
        if set(value) != {"kind", "identity"}:
            raise ContractError("native creation attempt fields invalid")
        return NativeCreationAttempt(identity_from_dict(value["identity"]))
    return BackendCreationAttempt(**value)


def matches_attempt(identity, attempt):
    from .backend_creation import BackendCreationAttempt
    from .backend_identity import LinuxUnitIdentity

    if type(attempt) is NativeCreationAttempt:
        return type(identity) is NativeProcessIdentity and identity == attempt.identity
    return (type(attempt) is BackendCreationAttempt and type(identity) is LinuxUnitIdentity
            and (identity.distribution, identity.unit) == (attempt.distribution, attempt.unit))
