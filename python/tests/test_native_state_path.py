import os
import sys
from pathlib import Path

import pytest

from ahc.domain.sessions import ContractError
from ahc.infrastructure.native_state_path import _mount_type, require_native_state_directory

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux mount identity")


def test_mount_observation_uses_longest_mount_and_exact_device():
    mounts = "1 0 8:1 / / rw - ext4 /dev/root rw\n2 1 0:2 / /work rw - 9p drvfs rw\n"
    assert _mount_type(Path("/state/data"), os.makedev(8, 1), mounts) == "ext4"
    assert _mount_type(Path("/work/data"), os.makedev(0, 2), mounts) == "9p"
    with pytest.raises(ValueError):
        _mount_type(Path("/work/data"), os.makedev(8, 1), mounts)


def test_actual_persistent_state_and_symlink_refusal(tmp_path):
    assert require_native_state_directory(tmp_path) in {"ext4", "xfs", "btrfs", "f2fs", "zfs", "bcachefs"}
    link = tmp_path / "link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ContractError):
        require_native_state_directory(link)
    with pytest.raises(ContractError):
        require_native_state_directory("/proc")
