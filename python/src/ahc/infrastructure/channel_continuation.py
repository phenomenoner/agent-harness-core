"""Offline atomic ordinary continuation of one channel consumer and its session."""

import hashlib
import json
from contextlib import closing
from dataclasses import replace

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.working_sets import validate_working_set_transition

from .runtime_restore_guard import offline_restore_ownership
from .schedule_repository import ScheduleRepository
from .telegram_inbox import TelegramInbox
from .working_db import insert_snapshot, open_connection, require_schema
from .working_repository import WorkingSetConflict


def _exists(connection, table):
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def retained_telegram_holds_summary(connection, expected):
    """Prove inert quarantine custody without changing or promoting any held bytes.

    Only consumed updates for this exact consumer or its verified ordinary
    ancestors qualify. Unknown ownership, future input and malformed custody
    remain blockers. The scan is bounded to 4096 rows and 8 MiB of payload.
    """
    empty = {"count": 0, "rows_digest": digest_json([])}
    if not _exists(connection, "runtime_telegram_holds"):
        return empty
    count, size = connection.execute(
        "SELECT count(*),coalesce(sum(length(update_json)),0) FROM runtime_telegram_holds"
    ).fetchone()
    if count == 0:
        return empty
    if count > 4096 or size > 8 * 1024 * 1024 or not _exists(
            connection, "runtime_telegram_cursors"):
        return None
    from .runtime_recovery_access import verified_continuation_edges

    summaries, edges = [], None
    try:
        for row in connection.execute(
                "SELECT * FROM runtime_telegram_holds ORDER BY account_id,update_id"):
            cursor = connection.execute(
                "SELECT * FROM runtime_telegram_cursors WHERE account_id=?",
                (row["account_id"],),
            ).fetchone()
            if (cursor is None or type(row["update_id"]) is not int
                    or type(cursor["next_offset"]) is not int
                    or not 0 <= row["update_id"] < cursor["next_offset"] < 2**63
                    or type(row["update_json"]) is not bytes):
                return None
            old = tuple(ChannelRoute.from_dict(v) for v in json.loads(row["routes_json"]))
            current = tuple(ChannelRoute.from_dict(v) for v in json.loads(cursor["routes_json"]))
            if (not old or not current
                    or any(r.session != expected.session for r in current)
                    or any(r.account_id != row["account_id"] for r in old + current)
                    or any(r.platform != "telegram" for r in old + current)
                    or len({(r.channel_id, r.user_id) for r in old}) != len(old)):
                return None
            for route in old:
                if route.session == expected.session:
                    continue
                if edges is None:
                    edges = verified_continuation_edges(
                        connection, expected.session.ref.virtual_session_id)
                ancestor = expected.session
                for _ in range(len(edges)):
                    ancestor = edges.get(ancestor)
                    if ancestor is None or ancestor == route.session:
                        break
                if ancestor != route.session:
                    return None
            normalized = canonical_json([r.to_dict() for r in sorted(
                (replace(r, session=expected.session) for r in old),
                key=lambda r: (r.channel_id, r.user_id))]).decode()
            if normalized != cursor["routes_json"]:
                return None
            payload = json.loads(row["update_json"])
            if (type(payload) is not dict or type(payload.get("update_id")) is not int
                    or payload["update_id"] != row["update_id"]
                    or canonical_json(payload) != row["update_json"]):
                return None
            summaries.append({"account_id": row["account_id"], "update_id": row["update_id"],
                              "routes_json": row["routes_json"],
                              "payload_digest": hashlib.sha256(row["update_json"]).hexdigest()})
    except (ValueError, TypeError, KeyError, UnicodeError, ContractError):
        return None
    return {"count": count, "rows_digest": digest_json(summaries)}


def continuation_blockers(connection, expected):
    """One store-wide rule set for advisory preflight and locked commit checks.

    Held owners are expected while a persistent workspace is open. A preflight
    lists them; only the offline commit requires every blocker to be absent.
    """
    blockers = []
    for table, predicate in (
        ("runtime_turns", "status='submitted'"),
        ("runtime_channel_outbox", "state IN ('pending','claimed','unknown')"),
        ("runtime_channel_attachments", "state IN ('pending','claimed','unknown')"),
        ("runtime_channel_progress", "state='claimed'"),
        ("runtime_children", "state IN ('queued','claimed')"),
        ("backend_custody", "state='held'"),
        ("workspace_custody", "state='held'"),
        ("backend_creation_intents", "state='pending'"),
        ("workspace_heads", "state='executing'"),
    ):
        if _exists(connection, table) and connection.execute(
            f"SELECT 1 FROM {table} WHERE {predicate} LIMIT 1"
        ).fetchone():
            blockers.append(table)
    if retained_telegram_holds_summary(connection, expected) is None:
        blockers.append("runtime_telegram_holds")
    if _exists(connection, "runtime_submissions"):
        held_filter = ("AND NOT EXISTS (SELECT 1 FROM channel_held h "
                       "WHERE h.operation_id=s.operation_id) "
                       if _exists(connection, "channel_held") else "")
        pending = connection.execute(
            "SELECT 1 FROM runtime_submissions s WHERE session_json=? "
            "AND NOT EXISTS (SELECT 1 FROM runtime_turns t "
            "WHERE t.operation_id=s.operation_id) " + held_filter + "LIMIT 1",
            (canonical_json(expected.session.to_dict()).decode(),),
        ).fetchone()
        if pending:
            blockers.append("undispatched_submissions")
    return tuple(blockers)


def _drained(connection, expected):
    blockers = continuation_blockers(connection, expected)
    if blockers:
        if blockers[0] == "undispatched_submissions":
            raise ContractError("channel continuation has undispatched submissions")
        raise ContractError("channel continuation requires drained custody: " + blockers[0])


def read_skill_continuation_receipt(store, expected):
    """Historical commit evidence only; does not authorize selecting an old config."""
    with store._read() as connection:
        if not _exists(connection, "runtime_skill_continuations"):
            return None
        row = connection.execute(
            "SELECT receipt_json,receipt_digest FROM runtime_skill_continuations "
            "WHERE predecessor_digest=?", (expected.snapshot_digest,)).fetchone()
    if row is None:
        return None
    receipt = json.loads(row[0])
    if digest_json(receipt) != row[1] or receipt["predecessor_digest"] != expected.snapshot_digest:
        raise ContractError("skill continuation receipt identity or digest mismatch")
    return receipt



def _migrate_schedules(connection, expected, successor, routes):
    if not _exists(connection, "runtime_schedules"):
        return 0
    old_session = canonical_json(expected.session.to_dict()).decode()
    new_session = canonical_json(successor.session.to_dict()).decode()
    mapping = {canonical_json(route.to_dict()).decode(): canonical_json(
        replace(route, session=successor.session).to_dict()).decode() for route in routes}
    count = 0
    rows = connection.execute(
        "SELECT * FROM runtime_schedules WHERE session_json=?", (old_session,)
    ).fetchall()
    for row in rows:
        old = ScheduleRepository._decode(row)
        new = replace(old, session=successor.session, revision=old.revision + 1)
        if _exists(connection, "runtime_channel_schedules"):
            binding = connection.execute(
                "SELECT route_json FROM runtime_channel_schedules WHERE schedule_id=?",
                (old.schedule_id,),
            ).fetchone()
            if binding is not None:
                if binding[0] not in mapping:
                    raise ContractError("continuation schedule has an unbound external route")
                connection.execute(
                    "UPDATE runtime_channel_schedules SET route_json=? WHERE schedule_id=?",
                    (mapping[binding[0]], old.schedule_id),
                )
        changed = connection.execute(
            "UPDATE runtime_schedules SET session_json=?,revision=?,schedule_json=? "
            "WHERE schedule_id=? AND revision=? AND session_json=?",
            (new_session, new.revision, canonical_json(new.to_dict()).decode(),
             old.schedule_id, old.revision, old_session),
        )
        if changed.rowcount != 1:
            raise WorkingSetConflict("continuation schedule CAS lost authority")
        count += 1
    return count


def _copy_preferences(connection, routes, successor):
    if not _exists(connection, "channel_preferences"):
        return 0
    count = 0
    for route in routes:
        old_key = canonical_json(route.to_dict()).decode()
        new_key = canonical_json(replace(route, session=successor.session).to_dict()).decode()
        old = connection.execute(
            "SELECT payload_json FROM channel_preferences WHERE route_json=?", (old_key,)
        ).fetchone()
        if old is None:
            continue
        present = connection.execute(
            "SELECT payload_json FROM channel_preferences WHERE route_json=?", (new_key,)
        ).fetchone()
        if present is not None and present[0] != old[0]:
            raise ContractError("continuation successor preferences conflict")
        connection.execute("INSERT OR IGNORE INTO channel_preferences VALUES(?,?)",
                           (new_key, old[0]))
        count += 1
    return count


def continue_telegram_session(store, supervisor_lock, expected, old_routes, *,
                              expected_offset, reason="backend-rollover",
                              fail_before_commit=False, skill_repository=None,
                              expected_skill_revisions=None):
    """Preserve the Telegram consumer cursor through the shared continuation."""
    return _continue_channel_session(
        store, supervisor_lock, expected, old_routes, platform="telegram",
        expected_position=expected_offset, reason=reason,
        fail_before_commit=fail_before_commit, skill_repository=skill_repository,
        expected_skill_revisions=expected_skill_revisions)


def continue_discord_session(store, supervisor_lock, expected, old_routes, *,
                             expected_resume, reason="backend-rollover",
                             fail_before_commit=False, skill_repository=None,
                             expected_skill_revisions=None):
    """Preserve the exact Gateway checkpoint without replaying an event."""
    return _continue_channel_session(
        store, supervisor_lock, expected, old_routes, platform="discord",
        expected_position=expected_resume, reason=reason,
        fail_before_commit=fail_before_commit, skill_repository=skill_repository,
        expected_skill_revisions=expected_skill_revisions)


def _continue_channel_session(store, supervisor_lock, expected, old_routes, *,
                              platform, expected_position, reason,
                              fail_before_commit, skill_repository,
                              expected_skill_revisions):
    """Preserve external route identity and cursor; never replay or reset effects.

    The exact expected snapshot is the reconciliation key. Repeating it after a
    committed transition reports already-applied; it cannot advance again.
    """
    old_routes = tuple(old_routes)
    if (skill_repository is None) != (expected_skill_revisions is None):
        raise ContractError("continuation requires both skill repository and exact revisions")
    target_json = None
    if skill_repository is not None:
        if (type(expected_skill_revisions) is not tuple
                or any(type(value) is not str for value in expected_skill_revisions)):
            raise ContractError("expected skill revisions must be an exact tuple")
        target_json = canonical_json({"root": str(skill_repository.root),
            "revisions": sorted(expected_skill_revisions)}).decode()
    if (not old_routes or any(type(route) is not ChannelRoute or
            route.platform != platform or route.session != expected.session
            for route in old_routes)):
        raise ContractError(f"continuation requires exact original {platform.title()} routes")
    if expected.status != "active" or expected.active_turn_id is not None:
        raise ContractError("continuation requires an idle active working set")
    successor = expected.continue_as(expected.session.successor(), reason)
    validate_working_set_transition(expected, successor)
    new_routes = tuple(replace(route, session=successor.session) for route in old_routes)
    consumer = None
    if platform == "telegram":
        old, new = TelegramInbox(store, old_routes), TelegramInbox(store, new_routes)
        old_routes_json, new_routes_json = old._routes_json, new._routes_json
        def position(value, connection):
            return value._offset(connection)
    else:
        from .discord_inbox import DiscordInbox
        from .discord_resume import DiscordResume

        if expected_position is not None:
            if (type(expected_position) is not dict or set(expected_position) != {
                    "session_id", "resume_url", "sequence"}):
                raise ContractError("continuation requires an exact Discord resume checkpoint")
            DiscordResume._validate(expected_position["session_id"],
                                    expected_position["resume_url"],
                                    expected_position["sequence"])
            expected_position = json.loads(canonical_json(expected_position))
        old = DiscordResume(DiscordInbox(store, old_routes))
        new = DiscordResume(DiscordInbox(store, new_routes))
        old_routes_json, new_routes_json = old._routes, new._routes
        def position(value, connection):
            return value._read(connection)
        consumer = {"platform": platform, "account_id": old.inbox.account_id,
                    "routes": json.loads(old_routes_json), "resume": expected_position}
        # The existing receipt also binds a Discord consumer with no skill target.
        # This prevents an absent Gateway checkpoint from making retry ownership
        # ambiguous. No separate state machine or gateway event is created.
        if target_json is None:
            target_json = "null"
    with offline_restore_ownership(store, supervisor_lock):
        with closing(open_connection(store.db_path, readonly=False)) as connection:
            require_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                recorded = None
                if target_json is not None:
                    connection.execute("CREATE TABLE IF NOT EXISTS runtime_skill_continuations ("
                        "predecessor_digest TEXT PRIMARY KEY,successor_digest TEXT NOT NULL,"
                        "target_json TEXT NOT NULL,receipt_json TEXT NOT NULL,"
                        "receipt_digest TEXT NOT NULL)")
                if _exists(connection, "runtime_skill_continuations"):
                    recorded = connection.execute("SELECT successor_digest,target_json,"
                        "receipt_json,receipt_digest "
                        "FROM runtime_skill_continuations WHERE predecessor_digest=?",
                        (expected.snapshot_digest,)).fetchone()
                current = store._load_current(connection, expected.session.ref.virtual_session_id)
                if current == successor:
                    if position(new, connection) != expected_position:
                        raise WorkingSetConflict("committed continuation cursor changed")
                    if recorded is not None and target_json != recorded[1]:
                        raise ContractError("committed continuation skill target must be retained")
                    if skill_repository is not None:
                        if recorded is None or tuple(recorded[:2]) != (
                                successor.snapshot_digest, target_json):
                            raise ContractError(
                                "committed continuation has a different or unbound skill target")
                        skill_repository.pin_revision_availability(
                            successor.session, expected_revisions=expected_skill_revisions)
                    retained = retained_telegram_holds_summary(connection, successor)
                    if retained is None:
                        raise ContractError("committed continuation quarantine custody changed")
                    if recorded is not None:
                        receipt = json.loads(recorded[2])
                        if (digest_json(receipt) != recorded[3] or
                                receipt.get("retained_telegram_holds", {
                                    "count": 0, "rows_digest": digest_json([])}) != retained):
                            raise ContractError("committed continuation quarantine receipt changed")
                        if consumer is not None and receipt.get("consumer") != consumer:
                            raise ContractError("committed continuation consumer binding changed")
                    elif consumer is not None:
                        raise ContractError("committed continuation has no consumer receipt")
                    connection.rollback()
                    return {"state": "already-applied", "working_set": successor.to_dict()}
                if current != expected:
                    raise WorkingSetConflict("continuation expected snapshot is stale")
                if recorded is not None:
                    raise ContractError(
                        "skill continuation receipt conflicts with current predecessor")
                _drained(connection, expected)
                if position(old, connection) != expected_position:
                    raise WorkingSetConflict("continuation expected cursor changed")
                if skill_repository is not None:
                    # The skill store is a separate transaction domain. Reserve
                    # this exact successor pin before committing runtime state.
                    # A failed runtime commit leaves an inert, immutable pin;
                    # retry must use the same revisions, never rewrite the pin.
                    skill_repository.pin_revision_availability(
                        successor.session, expected_revisions=expected_skill_revisions)
                schedules = _migrate_schedules(connection, expected, successor, old_routes)
                preferences = _copy_preferences(connection, old_routes, successor)
                retained = retained_telegram_holds_summary(connection, expected)
                insert_snapshot(connection, successor)
                updated = connection.execute(
                    "UPDATE current_working_sets SET generation=?,revision=?,agent_id=?,"
                    "route_id=?,snapshot_digest=? WHERE virtual_session_id=? AND snapshot_digest=?",
                    (successor.session.ref.generation, successor.revision,
                     successor.session.agent_id, successor.session.route_id,
                     successor.snapshot_digest, expected.session.ref.virtual_session_id,
                     expected.snapshot_digest),
                )
                if updated.rowcount != 1:
                    raise WorkingSetConflict("continuation snapshot CAS lost authority")
                if platform == "telegram":
                    cursor = connection.execute(
                        "UPDATE runtime_telegram_cursors SET routes_json=? "
                        "WHERE account_id=? AND routes_json=? AND next_offset IS ?",
                        (new_routes_json, old.account_id, old_routes_json, expected_position),
                    )
                    if cursor.rowcount != 1:
                        raise WorkingSetConflict("continuation consumer CAS lost authority")
                elif expected_position is not None:
                    checkpoint = connection.execute(
                        "UPDATE runtime_discord_resume SET routes_json=? "
                        "WHERE account_id=? AND routes_json=? AND session_id=? "
                        "AND resume_url=? AND sequence=?",
                        (new_routes_json, old.inbox.account_id, old_routes_json,
                         expected_position["session_id"], expected_position["resume_url"],
                         expected_position["sequence"]),
                    )
                    if checkpoint.rowcount != 1:
                        raise WorkingSetConflict("continuation consumer CAS lost authority")
                if fail_before_commit:
                    raise RuntimeError("injected continuation precommit failure")
                receipt = {"state": "applied", "working_set": successor.to_dict(),
                           "migrated_schedules": schedules,
                           "copied_preferences": preferences, "retained_telegram_holds": retained}
                receipt["cursor" if platform == "telegram" else "resume"] = expected_position
                if consumer is not None:
                    receipt["consumer"] = consumer
                if target_json is not None:
                    receipt["predecessor_digest"] = expected.snapshot_digest
                    if skill_repository is not None:
                        receipt["skill_target"] = json.loads(target_json)
                    connection.execute("INSERT INTO runtime_skill_continuations VALUES(?,?,?,?,?)",
                        (expected.snapshot_digest, successor.snapshot_digest, target_json,
                         canonical_json(receipt).decode(), digest_json(receipt)))
                connection.commit()
                return receipt
            except BaseException:
                connection.rollback()
                raise
