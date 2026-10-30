"""Narrow same-thread workspace grant; ordinary execution locks stay non-reentrant."""

import re
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from threading import Lock, get_ident

from ahc.domain.sessions import ContractError

_HELD = ContextVar("ahc_runtime_held_lock", default=None)
_TURN = ContextVar("ahc_runtime_workspace_grant", default=None)
_STARTUP = ContextVar("ahc_runtime_workspace_startup", default=None)
_EVALUATION = ContextVar("ahc_runtime_evaluation", default=None)


@dataclass
class _Grant:
    store: object
    thread: int
    request: object = None
    active: bool = True
    scope: object = None


@contextmanager
def evaluation_workspace(store, session, workspace_id):
    """Borrow only a live parent's lease for one disposable evaluation workspace."""
    if _EVALUATION.get() is not None:
        raise ContractError("nested evaluation scope unsupported")
    turn, held = _TURN.get(), _HELD.get()
    if turn is None:
        yield None
        return
    permits_workspace_scope(store, session, turn.request.workspace_scope)
    if (not re.fullmatch(r"eval-[0-9a-f]{64}", workspace_id)
            or workspace_id == turn.request.workspace_scope):
        raise ContractError("evaluation requires a distinct disposable workspace")
    remaining = int(turn.request.effective_deadline - time.monotonic())
    if remaining < 1:
        raise ContractError("evaluation parent deadline expired")
    from ahc.domain.runtime_turn import request_fingerprint

    durable = store.read_turn(session, turn.request.operation_id)
    current = store.read_current(session.ref.virtual_session_id)
    if (durable is None or durable.status != "submitted"
            or durable.request_digest != request_fingerprint(turn.request)
            or current.session != session or current.status != "active"
            or current.active_turn_id != turn.request.operation_id):
        raise ContractError("evaluation requires the current durable parent")
    grant = _Grant(store, get_ident(), (session, workspace_id, turn, held))
    token = _EVALUATION.set(grant)
    try:
        yield remaining
    finally:
        grant.active = False
        _EVALUATION.reset(token)


def permits_evaluation_workspace(store, session, workspace_id):
    grant = _EVALUATION.get()
    if grant is None:
        return False
    owner, name, turn, held = grant.request
    if (not grant.active or grant.store is not store or grant.thread != get_ident()
            or _TURN.get() is not turn or _HELD.get() is not held
            or not turn.active or not held.active):
        raise ContractError("evaluation scope expired or foreign")
    return owner == session and name == workspace_id


class ChildWorkerPermit:
    """One worker borrows a live parent lease without borrowing workspace authority.

    The owner must join the worker before leaving its parent execution scope.
    This permit rejects late use; it cannot interrupt an already-running effect.
    """

    def __init__(self, store, envelope):
        from ahc.domain.child_work import ChildWorkEnvelope

        if (type(envelope) is not ChildWorkEnvelope
                or not permits_child_parent(store, envelope.parent)):
            raise ContractError("child worker requires an active parent")
        if envelope.execution_kind != "subagent":
            raise ContractError("child worker supports ordinary children only")
        self._store, self._envelope = store, envelope
        self._held, self._turn = _HELD.get(), _TURN.get()
        self._lock, self._thread = Lock(), None
        self._used = self._active = False

    @contextmanager
    def activate(self):
        with self._lock:
            if self._used or not self._held.active or not self._turn.active:
                raise ContractError("child worker permit expired or used")
            self._used = self._active = True
            self._thread = get_ident()
        try:
            yield
        finally:
            self._active = False

    def require(self, store, envelope):
        if (not self._active or not self._held.active or not self._turn.active
                or self._thread != get_ident() or store is not self._store
                or envelope != self._envelope):
            raise ContractError("child worker scope mismatch or expired")


@contextmanager
def held_execution(store, *, scope=None):
    grant = _Grant(store, get_ident(), scope=scope)
    token = _HELD.set(grant)
    try:
        yield
    finally:
        grant.active = False
        _HELD.reset(token)


@contextmanager
def workspace_turn(store, request):
    held = _HELD.get()
    if held is None or not held.active or held.store is not store or held.thread != get_ident():
        raise ContractError("workspace turn requires owned execution lock")
    if held.scope is not None and held.scope != (request.session, request.workspace_scope):
        raise ContractError("workspace turn differs from the owned execution scope")
    if _TURN.get() is not None:
        raise ContractError("nested workspace turn grant unsupported")
    grant = _Grant(store, get_ident(), request)
    token = _TURN.set(grant)
    try:
        yield
    finally:
        grant.active = False
        _TURN.reset(token)


def permits_workspace(store, identity):
    return permits_workspace_scope(store, identity.session, identity.workspace_id)


def workspace_request(store, session, workspace_id):
    if not permits_workspace_scope(store, session, workspace_id):
        raise ContractError("workspace batch requires an admitted turn")
    return _TURN.get().request


def permits_child_parent(store, parent):
    """Reuse a parent's live turn lease only for its exact request and thread."""
    from ahc.domain.runtime_turn import request_fingerprint

    turn, held = _TURN.get(), _HELD.get()
    if turn is None:
        return False
    if (
        not turn.active
        or turn.store is not store
        or turn.thread != get_ident()
        or held is None
        or not held.active
        or held.store is not store
        or held.thread != get_ident()
        or turn.request.session != parent.session
        or turn.request.operation_id != parent.operation_id
        or request_fingerprint(turn.request) != parent.request_digest
    ):
        raise ContractError("child dispatch is outside the active parent execution scope")
    return True


def permits_workspace_scope(store, session, workspace_id):
    turn = _TURN.get()
    if turn is None:
        return False
    held = _HELD.get()
    if (
        not turn.active
        or turn.store is not store
        or turn.thread != get_ident()
        or held is None
        or not held.active
        or held.store is not store
        or held.thread != get_ident()
        or turn.request.session != session
        or turn.request.workspace_scope != workspace_id
    ):
        raise ContractError("workspace tool is outside the active execution scope")
    return True


@contextmanager
def workspace_startup(store, identity):
    held = _HELD.get()
    if (
        held is None
        or not held.active
        or held.store is not store
        or held.thread != get_ident()
        or _STARTUP.get() is not None
        or (_TURN.get() is not None and not permits_evaluation_workspace(
            store, identity.session, identity.workspace_id))
    ):
        raise ContractError("workspace startup requires an exclusive owned execution scope")
    if (held.scope is not None and held.scope != (identity.session, identity.workspace_id)
            and not permits_evaluation_workspace(store, identity.session, identity.workspace_id)):
        raise ContractError("workspace startup differs from the owned execution scope")
    grant = _Grant(store, get_ident(), identity)
    token = _STARTUP.set(grant)
    try:
        yield
    finally:
        grant.active = False
        _STARTUP.reset(token)


def permits_workspace_startup(store, session, workspace_id):
    grant, held = _STARTUP.get(), _HELD.get()
    if grant is None:
        return False
    if (
        not grant.active
        or grant.store is not store
        or grant.thread != get_ident()
        or held is None
        or not held.active
        or held.store is not store
        or held.thread != get_ident()
        or grant.request.session != session
        or grant.request.workspace_id != workspace_id
    ):
        raise ContractError("workspace process is outside the active startup scope")
    return True
