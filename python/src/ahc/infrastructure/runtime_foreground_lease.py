"""Session lifetime lease acquired under the existing runtime execution gate."""

import portalocker

from ahc.domain.skill_values import digest_json

from .working_db import require_regular_file
from .working_repository import WorkingWriterBusy


def acquire_foreground_lease(store, session):
    path = store.root / ("runtime-foreground-" + digest_json(session.to_dict()) + ".lock")
    require_regular_file(path, allow_absent=True)
    lock = portalocker.Lock(str(path), mode="a+b",
                           flags=portalocker.LOCK_EX | portalocker.LOCK_NB, timeout=0)
    try:
        with store.execution_lock(session=session, workspace_id="runtime-owner"):
            lock.acquire()
    except portalocker.exceptions.LockException:
        raise WorkingWriterBusy("runtime root is reserved for offline recovery") from None
    return lock
