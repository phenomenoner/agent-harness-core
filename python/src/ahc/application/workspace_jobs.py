"""Optional same-turn long cells with durable intent and out-of-kernel observation."""

import json
import math
import threading
import time
from dataclasses import replace

from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import digest_json
from ahc.domain.workspace import WorkspaceIdentity
from ahc.domain.workspace_display import display_fields
from ahc.domain.workspace_job_lease import WorkspaceJobLease
from ahc.infrastructure import workspace_job_store as receipts


class JobAdmittedUnknown(RuntimeError):
    """The durable job may have executed; retain its verified observation handle."""

    def __init__(self, job_id, identity):
        super().__init__("workspace job result is unverified")
        self.job_id, self.identity = job_id, identity


class WorkspaceJobs:
    def __init__(self, workspace):
        self.workspace = workspace
        self._guard = threading.RLock()
        self._workers = {}
        self._current = None
        self._initialized = False

    @property
    def available(self):
        return (callable(getattr(self.workspace.port, "execute_job", None))
                and callable(getattr(self.workspace.port, "cancel_job", None)))

    @property
    def identity(self):
        with self._guard:
            return self._current["successor"] if self._current is not None else None

    def _initialize(self):
        if not self._initialized:
            receipts.initialize(self.workspace.store)
            self._initialized = True

    def start(self, expected, job_id, code, expected_seconds, wait_seconds):
        self.workspace.port.validate_execute(code)
        request_digest = digest_json({"code": code, "expected_seconds": expected_seconds})
        with self._guard:
            self._initialize()
            existing = receipts.read(self.workspace.store, expected, job_id)
            if existing is not None:
                if existing["request_digest"] != request_digest:
                    raise ContractError("workspace job id bound to another request")
                try:
                    return self.observe(expected, job_id, wait_seconds)
                except Exception:
                    raise JobAdmittedUnknown(job_id, WorkspaceIdentity.from_dict(
                        json.loads(existing["successor_json"]))) from None
        now = time.monotonic()
        from .workspace_budget import job_deadline

        hard = job_deadline(self.workspace.port, now)
        budget = getattr(self.workspace.port, "_turn_budget", None)
        estimate = min(expected_seconds, hard-now) if budget is not None else expected_seconds
        lease = WorkspaceJobLease(now, hard, estimate)
        lease.owner_budget = budget
        with self._guard:
            self._initialize()
            row, created = receipts.reserve(self.workspace.store, expected, job_id, request_digest)
            if created:
                slot = {"origin": expected, "successor": replace(expected, revision=expected.revision+1),
                        "job_id": job_id, "done": threading.Event(), "lease": lease,
                        "port": self.workspace.port}
                self._current = self._workers[job_id] = slot
                thread = threading.Thread(target=self._run, args=(slot, code), daemon=True)
                slot["thread"] = thread
                try:
                    thread.start()
                except BaseException:
                    receipts.finish(self.workspace.store, slot["successor"], job_id,
                                    {"reason": "worker-not-started"}, state="unknown")
                    self.workspace.unavailable = True
                    raise JobAdmittedUnknown(job_id, slot["successor"]) from None
        try:
            return self.observe(expected, job_id, wait_seconds)
        except Exception:
            raise JobAdmittedUnknown(job_id, WorkspaceIdentity.from_dict(
                json.loads(row["successor_json"]))) from None

    def _run(self, slot, code):
        try:
            result = slot["port"].execute_job(slot["origin"], code, slot["lease"])
            if (type(result) is not dict or result.get("identity") != slot["successor"].to_dict()
                    or type(result.get("ok")) is not bool
                    or type(result.get("output")) is not str
                    or len(result["output"]) > 4096
                    or any(0xD800 <= ord(c) <= 0xDFFF for c in result["output"])
                    or type(result.get("output_truncated")) is not bool):
                raise ContractError("workspace job result invalid")
            display_fields(result)
            with self._guard:
                committed = receipts.finish(
                    self.workspace.store, slot["successor"], slot["job_id"], result)
                if committed and self._current is slot:
                    self._current = None
                elif not committed:
                    self.workspace.unavailable = True
        except BaseException:
            with self._guard:
                slot["failure"] = True
                self.workspace.unavailable = True
                try:
                    receipts.finish(self.workspace.store, slot["successor"], slot["job_id"],
                                    {"reason": "execution-or-receipt-unknown"}, state="unknown")
                except Exception:
                    pass  # Durable admitted intent remains unknown; never dispatch it again.
        finally:
            try:
                if slot["lease"].cancel_requested:
                    # Completion can race with the side-channel request after
                    # the adapter's final receive. This is the original worker,
                    # outside execute_job's lock, closing its pinned port only.
                    if not slot["lease"].stopped.is_set():
                        try:
                            slot["port"].close()
                            slot["lease"].stopped.set()
                        except Exception:
                            pass
                    details = {"cancel_requested": True,
                               "execution_stopped": slot["lease"].stopped.is_set(),
                               "effects_unknown": True, "external_effects_undone": False}
                    with self._guard:
                        receipts.update_cancellation(self.workspace.store, slot["successor"],
                                                     slot["job_id"], details)
                        if self.workspace.port is slot["port"] and details["execution_stopped"]:
                            self.workspace.closed = True
            except Exception:
                self.workspace.unavailable = True
            finally:
                slot["done"].set()

    def observe(self, expected, job_id, wait_seconds=0):
        with self._guard:
            self._initialize()
            slot = self._workers.get(job_id)
        if slot is not None and wait_seconds:
            budget = getattr(slot["lease"], "owner_budget", None)
            until = time.monotonic() + wait_seconds
            while not slot["done"].is_set():
                now = time.monotonic()
                if now >= until:
                    break
                active = budget.current_deadline() if budget is not None else until
                if budget is not None and budget.status()["state"] == "finalizing":
                    break  # Return control so the model can use its wrap-up allowance.
                remaining = min(until - now, active - time.monotonic())
                if remaining <= 0 or slot["done"].wait(remaining):
                    break
        with self._guard:
            row = receipts.read(self.workspace.store, expected, job_id)
            if row is None:
                raise ContractError("workspace job not found")
            if row["state"] in ("admitted", "running") and slot is None:
                successor = WorkspaceIdentity.from_dict(json.loads(row["successor_json"]))
                receipts.finish(self.workspace.store, successor, job_id,
                                {"reason": "host-worker-unavailable-no-replay"}, state="unknown")
                row = receipts.read(self.workspace.store, expected, job_id)
            state = row["state"]
            state = "running" if state in ("admitted", "running") else state
            result = {
                "identity": self.workspace.identity.to_dict(), "job_id": job_id,
                "job_identity": json.loads(row["successor_json"]), "status": state,
                "replay_allowed": False,
            }
            if slot is not None:
                result["budget"] = slot["lease"].to_dict(time.monotonic())
            if row["result_json"] is not None:
                result["execution"] = json.loads(row["result_json"])
                if state == "cancelled":
                    result["cancellation"] = result["execution"]
            return result

    def cancel(self, expected, job_id):
        with self._guard:
            observed = self.observe(expected, job_id)
            if observed["status"] not in ("running", "unknown"):
                return observed
            slot = self._workers.get(job_id)
            if (slot is None or self._current is not slot
                    or slot["port"] is not self.workspace.port
                    or expected != slot["successor"]
                    or self.workspace.port.identity not in (slot["origin"], slot["successor"])):
                if observed["status"] == "unknown":
                    return observed  # Missing cleanup authority does not resolve prior effects.
                raise ContractError("workspace job cancellation owner unavailable")
            # The guard pins the exact port instance through original custody cleanup.
            # The adapter itself fences the native cgroup's device/inode/boot identity.
            details = {"cancel_requested": True, "execution_stopped": False,
                       "effects_unknown": True, "external_effects_undone": False}
            try:
                receipts.finish(self.workspace.store, slot["successor"], job_id,
                                details, state="cancelled")
            except Exception:
                # Receipt uncertainty cannot strand a requested cancellation.
                # The guard still pins this exact admitted worker and port.
                slot["lease"].request_cancel()
                try:
                    slot["port"].cancel_job(slot["lease"])
                except Exception:
                    pass
                self.workspace.unavailable = True
                raise JobAdmittedUnknown(job_id, slot["successor"]) from None
            # Only the exact worker owns native cleanup. close() takes its
            # execution lock and cannot serve as the side-channel cancel path.
            slot["lease"].request_cancel()
            try:
                slot["port"].cancel_job(slot["lease"])
            except Exception:
                pass
            self.workspace.unavailable = True
        # The worker needs _guard to commit; never wait while holding it.
        try:
            slot["done"].wait(1)
            with self._guard:
                details["execution_stopped"] = slot["lease"].stopped.is_set()
                if self.workspace.port is slot["port"]:
                    self.workspace.closed = details["execution_stopped"]
                receipts.update_cancellation(self.workspace.store, slot["successor"], job_id, details)
                observed = self.observe(expected, job_id)
                observed["cancellation"] = details
                return observed
        except Exception:
            self.workspace.unavailable = True
            raise JobAdmittedUnknown(job_id, slot["successor"]) from None

    def finish_turn(self):
        with self._guard:
            slot = self._current
        if slot is not None:
            self.cancel(slot["successor"], slot["job_id"])
            slot["done"].wait(1)


def invoke_job(workspace, arguments):
    try:
        if type(arguments) is not dict:
            raise ContractError("workspace job fields invalid")
        action = arguments.get("action")
        required = {"expected", "action", "job_id"}
        extra = ({"code", "expected_seconds", "wait_seconds"} if action == "start"
                 else {"wait_seconds"} if action == "wait" else set())
        if (action not in ("start", "status", "wait", "cancel")
                or not required <= set(arguments) or set(arguments) - required - extra):
            raise ContractError("workspace job fields invalid")
        expected = WorkspaceIdentity.from_dict(arguments["expected"])
        if expected != workspace.identity:
            raise ContractError("workspace job reference stale")
        job_id = _identifier(arguments["job_id"], "workspace job")
        wait = arguments.get("wait_seconds", 0)
        if type(wait) not in (int, float) or not math.isfinite(wait) or not 0 <= wait <= 30:
            raise ContractError("workspace job wait invalid")
        if action == "start":
            if not {"code", "expected_seconds"} <= set(arguments):
                raise ContractError("workspace job start fields missing")
            if workspace.closed or workspace.unavailable:
                raise ContractError("workspace job unavailable")
            result = workspace.jobs.start(expected, job_id, arguments["code"],
                                          arguments["expected_seconds"], wait)
        elif action == "cancel":
            result = workspace.jobs.cancel(expected, job_id)
        else:
            result = workspace.jobs.observe(expected, job_id, wait)
        unknown = result["status"] == "unknown"
        return {"disposition": "unknown" if unknown else "observed",
                "unresolved_effects": unknown or bool(result.get("cancellation", {}).get("effects_unknown")),
                "result": result}
    except JobAdmittedUnknown as error:
        return {"disposition": "unknown", "reason": "workspace-job-result-unverified",
                "unresolved_effects": True,
                "identity": workspace.identity.to_dict(),
                "result": {"identity": workspace.identity.to_dict(),
                           "job_identity": error.identity.to_dict(), "job_id": error.job_id,
                           "status": "unknown", "replay_allowed": False}}
    except Exception:
        return {"disposition": "rejected", "reason": "workspace-job-request-unavailable",
                "unresolved_effects": False}
