"""Transactional learning handoff; collection alone never starts a model call."""

import json
import time
from dataclasses import dataclass

from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json

BACKLOG_LIMIT = 128
RETENTION_MS = 7 * 24 * 60 * 60 * 1000
STATES = ('awaiting-result', 'awaiting-delivery', 'claimed', 'complete',
          'unknown', 'cancelled', 'expired', 'dropped')
TABLES = (
    """CREATE TABLE IF NOT EXISTS runtime_learning_sources (
    operation_id TEXT PRIMARY KEY REFERENCES runtime_turns(operation_id),
    session_json TEXT NOT NULL, scope_key TEXT NOT NULL, source_json TEXT NOT NULL,
    created_ms INTEGER NOT NULL, state TEXT NOT NULL,
    result_digest TEXT, facts_json TEXT, claim_token TEXT, binding_json TEXT,
    decision_json TEXT)""",
    """CREATE INDEX IF NOT EXISTS runtime_learning_pending
    ON runtime_learning_sources(session_json,state,created_ms)""",
    """CREATE TABLE IF NOT EXISTS runtime_learning_control (
    scope_key TEXT PRIMARY KEY, generation INTEGER NOT NULL, stopped INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runtime_learning_usage (
    scope_key TEXT NOT NULL, day INTEGER NOT NULL, tokens INTEGER NOT NULL,
    calls INTEGER NOT NULL, PRIMARY KEY(scope_key,day))""",
)


def _encoded(value):
    return canonical_json(value).decode()


def _exists(db):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                      ('runtime_learning_sources',)).fetchone() is not None


def prepare_source(db, request, *, now_ms=None):
    if 'workspace.learning' not in request.capabilities:
        return
    for statement in TABLES:
        db.execute(statement)
    now = int(time.time()*1000) if now_ms is None else now_ms
    route = db.execute('SELECT route_json FROM runtime_channel_routes WHERE operation_id=?',
                       (request.operation_id,)).fetchone()
    route = json.loads(route[0]) if route is not None else None
    # Daily/stop scope survives virtual-session generation changes.
    route_owner = {k:v for k,v in route.items() if k != 'session'} if route else None
    scope = digest_json([request.session.agent_id, request.session.route_id,
                         request.workspace_scope, route_owner])
    source = {'operation_id':request.operation_id, 'session':request.session.to_dict(),
              'workspace_scope':request.workspace_scope, 'route':route,
              'input_digest':request.input_digest,
              'selected_revisions':list(request.revision_digests)}
    db.execute("UPDATE runtime_learning_sources SET state='expired' "
               "WHERE scope_key=? AND state IN ('awaiting-result','awaiting-delivery') "
               'AND created_ms<?', (scope,now-RETENTION_MS))
    count = db.execute("SELECT count(*) FROM runtime_learning_sources WHERE scope_key=? "
                       "AND state IN ('awaiting-result','awaiting-delivery','claimed')",(scope,)).fetchone()[0]
    state = 'dropped' if count >= BACKLOG_LIMIT else 'awaiting-result'
    db.execute('INSERT OR IGNORE INTO runtime_learning_sources '
               '(operation_id,session_json,scope_key,source_json,created_ms,state) VALUES(?,?,?,?,?,?)',
               (request.operation_id,_encoded(request.session.to_dict()),scope,_encoded(source),now,state))


def settle_source(db, turn, result):
    if not _exists(db):
        return
    row = db.execute('SELECT * FROM runtime_learning_sources WHERE operation_id=?',
                     (turn['operation_id'],)).fetchone()
    if row is None:
        return
    if row['session_json'] != turn['session_json']:
        raise ContractError('learning result owner mismatch')
    facts = {'accepted':result.get('accepted') is True,
             'report_accepted':result.get('report_accepted') is True,
             'disposition':result.get('disposition'),
             'unresolved_effects':result.get('unresolved_effects') is True,
             'reason':result.get('reason'), 'answer_adoption_verified':False}
    cancelled = result.get('reason') in ('cancelled','user-cancelled','deadline-or-cancel')
    state = row['state']
    if state == 'awaiting-result':
        state = 'cancelled' if cancelled else 'awaiting-delivery'
    db.execute('UPDATE runtime_learning_sources SET state=?,result_digest=?,facts_json=? '
               'WHERE operation_id=?',(state,digest_json(result),_encoded(facts),turn['operation_id']))


@dataclass(frozen=True)
class ReflectionPolicy:
    job_timeout_seconds: int
    tokens_per_job: int
    daily_token_limit: int
    daily_call_limit: int
    model: str = 'gpt-6-luna'
    effort: str = 'max'

    def __post_init__(self):
        values=(self.job_timeout_seconds,self.tokens_per_job,self.daily_token_limit,self.daily_call_limit)
        if (any(type(v) is not int or v<=0 for v in values)
                or self.job_timeout_seconds>600 or self.tokens_per_job>self.daily_token_limit
                or type(self.model) is not str or not self.model or len(self.model)>128
                or not all(c.isascii() and (c.isalnum() or c in '-_./') for c in self.model)
                or self.effort not in ('low','medium','high','xhigh','max','ultra')):
            raise ContractError('reflection requires a finite exact-route owner policy')


class RuntimeLearningQueue:
    """Owner-only queue operations; not part of the dynamic/model tool API."""
    def __init__(self, store, *, clock=lambda:int(time.time()*1000)):
        self.store, self.clock = store, clock

    def get(self, session, operation_id):
        with self.store._read() as db:
            if not _exists(db):
                return None
            row=db.execute('SELECT * FROM runtime_learning_sources WHERE operation_id=?',
                           (operation_id,)).fetchone()
            if row is None:
                return None
            if row['session_json'] != _encoded(session.to_dict()):
                raise ContractError('learning source scope mismatch')
            return dict(row)

    def summary(self, session, *, reflection_enabled=False):
        with self.store._read() as db:
            rows = [] if not _exists(db) else db.execute(
                'SELECT state,count(*) FROM runtime_learning_sources WHERE session_json=? GROUP BY state',
                (_encoded(session.to_dict()),)).fetchall()
        return {'mode':'model-backed-reflection' if reflection_enabled else 'collect-only',
                'model_dispatch_configured':reflection_enabled,
                'counts':{r[0]:r[1] for r in rows},'backlog_limit':BACKLOG_LIMIT,
                'eligibility_window_ms':RETENTION_MS,'history_pruning_configured':False,
                'automatic_recovery_configured':False,'claim_deadline_kind':'completion-only',
                'answer_adoption_verified':False}

    def claim(self, session, operation_id, token, policy):
        if type(policy) is not ReflectionPolicy:
            raise ContractError('reflection has no configured finite model budget')
        _identifier(token,'learning owner')
        now=self.clock()
        def claim(db):
            row=db.execute('SELECT * FROM runtime_learning_sources WHERE operation_id=?',
                           (operation_id,)).fetchone()
            if row is None or row['session_json'] != _encoded(session.to_dict()):
                raise ContractError('learning source scope mismatch')
            current=self.store._load_current(db,session.ref.virtual_session_id)
            if current.session!=session or current.status!='active':
                raise ContractError('learning source session superseded')
            if row['state']!='awaiting-delivery':
                raise ContractError('learning source is not claimable; never replay')
            if now-row['created_ms']>=RETENTION_MS:
                db.execute("UPDATE runtime_learning_sources SET state='expired' WHERE operation_id=?",(operation_id,))
                return None
            outbox=db.execute('SELECT state,route_json FROM runtime_channel_outbox WHERE operation_id=?',
                              (operation_id,)).fetchone()
            source=json.loads(row['source_json'])
            if (outbox is None or outbox['state']!='sent'
                    or json.loads(outbox['route_json'])!=source['route']):
                return None  # Pending, claimed and unknown delivery never become adoption.
            control=db.execute('SELECT generation,stopped FROM runtime_learning_control WHERE scope_key=?',
                               (row['scope_key'],)).fetchone()
            if control is not None and control['stopped']:
                return None
            if db.execute("SELECT 1 FROM runtime_learning_sources WHERE scope_key=? AND state='claimed'",
                          (row['scope_key'],)).fetchone():
                return None
            day=now//86400000
            usage=db.execute('SELECT tokens,calls FROM runtime_learning_usage WHERE scope_key=? AND day=?',
                             (row['scope_key'],day)).fetchone()
            used,calls=(usage['tokens'],usage['calls']) if usage else (0,0)
            if used+policy.tokens_per_job>policy.daily_token_limit or calls+1>policy.daily_call_limit:
                return None
            generation=control['generation'] if control else 0
            binding={'owner':token,'job_id':'learning-'+digest_json([row['scope_key'],operation_id]),
                     'model':policy.model,'effort':policy.effort,'max_tokens':policy.tokens_per_job,
                     'deadline_ms':now+policy.job_timeout_seconds*1000,
                     'timeout_seconds':policy.job_timeout_seconds,
                     'cancel_generation':generation,'evidence_scope':
                     'delivered-answer' if json.loads(row['facts_json'])['accepted'] else 'execution-failure-and-delivered-report'}
            db.execute('INSERT INTO runtime_learning_usage VALUES(?,?,?,?) ON CONFLICT(scope_key,day) '
                       'DO UPDATE SET tokens=excluded.tokens,calls=excluded.calls',
                       (row['scope_key'],day,used+policy.tokens_per_job,calls+1))
            db.execute("UPDATE runtime_learning_sources SET state='claimed',claim_token=?,binding_json=? "
                       'WHERE operation_id=?',(token,_encoded(binding),operation_id))
            return {**binding,'source':source,'facts':json.loads(row['facts_json'])}
        return self.store._mutate(claim)

    def finish(self, session, operation_id, token, decision, *, model, effort):
        if (type(decision) is not dict or decision.get('action') not in
                ('no-op','propose','modify','merge','retire') or len(_encoded(decision))>32768):
            raise ContractError('learning decision invalid')
        def finish(db):
            row=db.execute('SELECT * FROM runtime_learning_sources WHERE operation_id=?',
                           (operation_id,)).fetchone()
            if (row is None or row['session_json']!=_encoded(session.to_dict())
                    or row['state']!='claimed' or row['claim_token']!=token):
                raise ContractError('learning result owner is stale')
            binding=json.loads(row['binding_json'])
            control=db.execute('SELECT generation,stopped FROM runtime_learning_control WHERE scope_key=?',
                               (row['scope_key'],)).fetchone()
            current=self.store._load_current(db,session.ref.virtual_session_id)
            from .runtime_channel_store import _exists as channel_table_exists

            held_filter = ("AND NOT EXISTS (SELECT 1 FROM channel_held h "
                           "WHERE h.operation_id=s.operation_id) "
                           if channel_table_exists(db, "channel_held") else "")
            pending=db.execute("SELECT 1 FROM runtime_submissions s "
                "LEFT JOIN runtime_turns t ON t.operation_id=s.operation_id "
                "WHERE s.session_json=? AND s.goal_digest=? AND t.operation_id IS NULL "
                + held_filter + "LIMIT 1",
                (_encoded(session.to_dict()),current.goal.digest)).fetchone()
            if (pending is not None or current.session!=session or current.status!='active'
                    or self.clock()>=binding['deadline_ms'] or model!=binding['model'] or effort!=binding['effort']
                    or control is not None and (control['stopped'] or control['generation']!=binding['cancel_generation'])):
                db.execute("UPDATE runtime_learning_sources SET state='unknown' WHERE operation_id=?",(operation_id,))
                return {'state':'unknown','replay_allowed':False}
            db.execute("UPDATE runtime_learning_sources SET state='complete',decision_json=? WHERE operation_id=?",
                       (_encoded(decision),operation_id))
            return {'state':'complete','automatic_activation':False,'benefit':'unmeasured'}
        return self.store._mutate(finish)

    def stop(self, session, operation_id):
        row=self.get(session,operation_id)
        if row is None:
            raise ContractError('learning source unavailable')
        def stop(db):
            db.execute('INSERT INTO runtime_learning_control VALUES(?,1,1) ON CONFLICT(scope_key) '
                       'DO UPDATE SET generation=generation+1,stopped=1',(row['scope_key'],))
            db.execute("UPDATE runtime_learning_sources SET state='cancelled' WHERE scope_key=? "
                       "AND state IN ('awaiting-result','awaiting-delivery','claimed')",(row['scope_key'],))
        self.store._mutate(stop)

    def reconcile(self, session):
        # Lost claimed owners are indeterminate; this never resubmits a model call.
        def reconcile(db):
            if _exists(db):
                db.execute("UPDATE runtime_learning_sources SET state='unknown' "
                           "WHERE session_json=? AND state='claimed'",(_encoded(session.to_dict()),))
        self.store._mutate(reconcile)

    def pending(self, session):
        with self.store._read() as db:
            if not _exists(db):
                return ()
            return tuple(row[0] for row in db.execute(
                "SELECT s.operation_id FROM runtime_learning_sources s "
                "JOIN runtime_channel_outbox o ON o.operation_id=s.operation_id "
                "WHERE s.session_json=? AND s.state='awaiting-delivery' AND o.state='sent' "
                "ORDER BY s.created_ms,s.operation_id LIMIT ?",
                (_encoded(session.to_dict()), BACKLOG_LIMIT)).fetchall())

    def claim_current(self, session, operation_id, token):
        with self.store._read() as db:
            row = db.execute("SELECT * FROM runtime_learning_sources WHERE operation_id=?",
                             (operation_id,)).fetchone()
            if (row is None or row["session_json"] != _encoded(session.to_dict())
                    or row["state"] != "claimed" or row["claim_token"] != token):
                return False
            binding = json.loads(row["binding_json"])
            control = db.execute("SELECT generation,stopped FROM runtime_learning_control WHERE scope_key=?",
                                 (row["scope_key"],)).fetchone()
            current = self.store._load_current(db, session.ref.virtual_session_id)
            return (current.session == session and current.status == "active"
                    and self.clock() < binding["deadline_ms"]
                    and (control is None or not control["stopped"]
                         and control["generation"] == binding["cancel_generation"]))

    def evidence(self, session, operation_id):
        row = self.get(session, operation_id)
        if row is None:
            raise ContractError("reflection source unavailable")
        from .runtime_inbox import RuntimeInbox
        with self.store._read() as db:
            raw = db.execute("SELECT * FROM runtime_submissions WHERE operation_id=?",
                             (operation_id,)).fetchone() if RuntimeInbox(self.store)._exists(db) else None
        submission = RuntimeInbox._decode(raw) if raw is not None else None
        if submission is not None and submission["session"] != session.to_dict():
            raise ContractError("reflection input source owner mismatch")
        turn = self.store.read_turn(session, operation_id)
        if turn is None or turn.result is None or digest_json(turn.result) != row["result_digest"]:
            raise ContractError("reflection source outcome unavailable")
        source = json.loads(row["source_json"])
        return {"source_operation": operation_id, "facts": json.loads(row["facts_json"]),
                "selected_revisions": source["selected_revisions"],
                "input": submission["input_text"] if submission else "",
                "output": str(turn.result.get("output_text", ""))}

    def abandon(self, session, operation_id, token, kind):
        def write(db):
            row = db.execute("SELECT * FROM runtime_learning_sources WHERE operation_id=?",
                             (operation_id,)).fetchone()
            if row is None or row["session_json"] != _encoded(session.to_dict()):
                raise ContractError("reflection source owner mismatch")
            if row["state"] == "claimed" and row["claim_token"] == token:
                db.execute("UPDATE runtime_learning_sources SET state='unknown',decision_json=? "
                           "WHERE operation_id=?",
                           (_encoded({"error_kind": kind, "replay_allowed": False}), operation_id))
                return "unknown"
            return row["state"]
        return self.store._mutate(write)


    def _guard_effect(self, db, session, operation, generation, *, deadline_ms=None):
        row = db.execute('SELECT * FROM runtime_learning_sources WHERE operation_id=?', (operation,)).fetchone()
        if row is None or row['session_json'] != _encoded(session.to_dict()):
            raise ContractError('learning follow-up source mismatch')
        source = json.loads(row['source_json'])
        facts = json.loads(row['facts_json']) if row['facts_json'] else {}
        outbox = db.execute('SELECT state,route_json FROM runtime_channel_outbox WHERE operation_id=?', (operation,)).fetchone()
        current = self.store._load_current(db, session.ref.virtual_session_id)
        control = db.execute('SELECT generation,stopped FROM runtime_learning_control WHERE scope_key=?', (row['scope_key'],)).fetchone()
        from .runtime_channel_store import _exists as channel_table_exists

        held_filter = ("AND NOT EXISTS (SELECT 1 FROM channel_held h "
                       "WHERE h.operation_id=s.operation_id) "
                       if channel_table_exists(db, "channel_held") else "")
        pending = db.execute("SELECT 1 FROM runtime_submissions s LEFT JOIN runtime_turns t ON t.operation_id=s.operation_id "
                             "WHERE s.session_json=? AND s.goal_digest=? AND t.operation_id IS NULL "
                             + held_filter + "LIMIT 1",
                             (_encoded(session.to_dict()), current.goal.digest)).fetchone()
        if (current.session != session or current.status != 'active' or pending is not None
                or facts.get('accepted') is not True or facts.get('unresolved_effects') is not False
                or outbox is None or outbox['state'] != 'sent' or json.loads(outbox['route_json']) != source['route']
                or control is not None and (control['stopped'] or control['generation'] != generation)
                or control is None and generation != 0
                or deadline_ms is not None and self.clock() >= deadline_ms):
            raise ContractError('learning follow-up revoked, uncertain or superseded')
        return row

    def guard_effect(self, session, operation, generation, action, *, deadline_ms=None):
        """Hold the existing runtime writer through the scoped skill commit.

        New input and stop writes cannot interleave the observed guard and effect.
        A lost return is reconciled from the skill receipt, never replayed as work.
        """
        def guarded(db):
            self._guard_effect(db, session, operation, generation, deadline_ms=deadline_ms)
            return action()
        return self.store._mutate(guarded)

    def reserve_followup(self, session, operation, generation, policy, job_id, *, deadline_ms):
        if type(policy) is not ReflectionPolicy:
            raise ContractError('comparison needs finite configured model policy')
        _identifier(job_id, 'comparison job')
        def reserve(db):
            source = self._guard_effect(db, session, operation, generation, deadline_ms=deadline_ms)
            db.execute('CREATE TABLE IF NOT EXISTS runtime_learning_calls (job_id TEXT PRIMARY KEY, '
                       'session_json TEXT NOT NULL, source_operation TEXT NOT NULL, state TEXT NOT NULL, '
                       'binding_json TEXT NOT NULL, output_text TEXT)')
            if db.execute('SELECT 1 FROM runtime_learning_calls WHERE job_id=?', (job_id,)).fetchone():
                return None  # An earlier admission, including UNKNOWN, is never dispatched twice.
            now = self.clock()
            day = now // 86400000
            usage = db.execute('SELECT tokens,calls FROM runtime_learning_usage WHERE scope_key=? AND day=?',
                               (source['scope_key'], day)).fetchone()
            tokens, calls = (usage['tokens'], usage['calls']) if usage else (0, 0)
            if tokens + policy.tokens_per_job > policy.daily_token_limit or calls + 1 > policy.daily_call_limit:
                return None
            timeout = min(policy.job_timeout_seconds, (deadline_ms - now) / 1000)
            if timeout <= 0:
                return None
            binding = {'job_id': job_id, 'model': policy.model, 'effort': policy.effort,
                       'max_tokens': policy.tokens_per_job, 'timeout_seconds': timeout,
                       'deadline_ms': min(deadline_ms, now + policy.job_timeout_seconds * 1000),
                       'cancel_generation': generation}
            db.execute('INSERT INTO runtime_learning_usage VALUES(?,?,?,?) ON CONFLICT(scope_key,day) '
                       'DO UPDATE SET tokens=excluded.tokens,calls=excluded.calls',
                       (source['scope_key'], day, tokens + policy.tokens_per_job, calls + 1))
            db.execute('INSERT INTO runtime_learning_calls VALUES(?,?,?,?,?,NULL)',
                       (job_id, _encoded(session.to_dict()), operation, 'claimed', _encoded(binding)))
            return binding
        return self.store._mutate(reserve)

    def finish_followup(self, session, operation, binding, output):
        if type(output) is not str or not output or len(output) > 100000:
            raise ContractError('comparison output unavailable')
        def finish(db):
            self._guard_effect(db, session, operation, binding['cancel_generation'], deadline_ms=binding['deadline_ms'])
            changed = db.execute("UPDATE runtime_learning_calls SET state='complete',output_text=? "
                                 "WHERE job_id=? AND session_json=? AND source_operation=? AND state='claimed' AND binding_json=?",
                                 (output, binding['job_id'], _encoded(session.to_dict()), operation, _encoded(binding))).rowcount
            if not changed:
                raise ContractError('comparison result owner changed')
        self.store._mutate(finish)

    def reconcile_followups(self, session):
        def reconcile(db):
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_learning_calls'").fetchone():
                db.execute("UPDATE runtime_learning_calls SET state='unknown' WHERE session_json=? AND state='claimed'",
                           (_encoded(session.to_dict()),))
        self.store._mutate(reconcile)

    def delivered_sources(self, session):
        with self.store._read() as db:
            if not _exists(db):
                return ()
            return tuple(row[0] for row in db.execute(
                "SELECT s.operation_id FROM runtime_learning_sources s JOIN runtime_channel_outbox o ON o.operation_id=s.operation_id "
                "WHERE s.session_json=? AND o.state='sent' AND s.result_digest IS NOT NULL ORDER BY s.created_ms DESC LIMIT ?",
                (_encoded(session.to_dict()), BACKLOG_LIMIT)))


    def route_for_operation(self, session, operation):
        """The immutable ingress tuple, never a model-selected route."""
        from ahc.domain.channel_ingress import ChannelRoute
        with self.store._read() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_channel_routes'").fetchone():
                return None
            row = db.execute('SELECT route_json FROM runtime_channel_routes WHERE operation_id=?', (operation,)).fetchone()
        if row is None:
            return None
        route = ChannelRoute.from_dict(json.loads(row['route_json']))
        if route.session != session:
            raise ContractError('scoped catalog source route owner mismatch')
        return route.to_dict()
