"""Operation-frozen, conversation-scoped skill trials over the existing skill store."""

import json

from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.skill_comparison import BehaviorCase, assess_comparison, suite_digest
from ahc.domain.skill_graph import validate_active_graph
from ahc.domain.skill_values import canonical_json, digest_json
from .skill_repository_graph import load_verified_revision_catalog

TABLES = (
    "CREATE TABLE IF NOT EXISTS learning_scoped_heads (context_id TEXT PRIMARY KEY, "
    "context_json TEXT NOT NULL, generation INTEGER NOT NULL, overrides_json TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS learning_scoped_trials (trial_id TEXT PRIMARY KEY, "
    "context_id TEXT NOT NULL, state TEXT NOT NULL, plan_json TEXT NOT NULL, "
    "outputs_json TEXT, assessment_json TEXT, adoption_generation INTEGER, "
    "previous_json TEXT, after_json TEXT)",
    "CREATE TABLE IF NOT EXISTS learning_operation_catalogs (session_id TEXT NOT NULL, "
    "generation INTEGER NOT NULL, operation_id TEXT NOT NULL, session_json TEXT NOT NULL, "
    "revisions_json TEXT NOT NULL, epoch INTEGER NOT NULL, catalog_digest TEXT NOT NULL, "
    "PRIMARY KEY(session_id,generation,operation_id))",
    "CREATE TABLE IF NOT EXISTS learning_scoped_observations (trial_id TEXT NOT NULL, "
    "operation_id TEXT NOT NULL, observation_json TEXT NOT NULL, "
    "PRIMARY KEY(trial_id,operation_id))",
)


def encoded(value):
    return canonical_json(value).decode()


def stable_route(route):
    if route is None:
        return None
    value = ChannelRoute.from_dict(route).to_dict()
    return {key: item for key, item in value.items() if key != 'session'}


def context_value(session, route=None):
    if route is not None:
        bound = ChannelRoute.from_dict(route).session
        if (bound.agent_id != session.agent_id or bound.route_id != session.route_id
                or bound.ref.virtual_session_id != session.ref.virtual_session_id):
            raise ContractError('physical skill route belongs to another virtual owner')
    return [session.agent_id, session.route_id, session.ref.virtual_session_id, stable_route(route)]


def context_id(session, route=None):
    return digest_json(context_value(session, route))


def exists(db, table):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def head(db, session, route=None):
    row = db.execute("SELECT * FROM learning_scoped_heads WHERE context_id=?", (context_id(session, route),)).fetchone()
    if row is None:
        return 0, {}
    if row['context_json'] != encoded(context_value(session, route)):
        raise ContractError('scoped skill context mismatch')
    return row['generation'], json.loads(row['overrides_json'])


def apply_overrides(base, overrides, catalog):
    by_digest = {r.revision_digest: r for r in catalog}
    current = {r.skill_name: r for r in base}
    for name, revision in overrides.items():
        if revision is None:
            current.pop(name, None)
        else:
            value = by_digest.get(revision)
            if value is None or value.skill_name != name:
                raise ContractError('scoped skill revision mismatch')
            current[name] = value
    values = tuple(sorted(current.values(), key=lambda r: r.skill_name))
    if validate_active_graph(values, catalog).issues:
        raise ContractError('scoped skill dependency or conflict invalid')
    return values


def read_operation_catalog(repository, session, operation):
    with repository._read() as db:
        if not exists(db, 'learning_operation_catalogs'):
            return None
        row = db.execute("SELECT * FROM learning_operation_catalogs WHERE session_id=? AND generation=? AND operation_id=?",
                         (session.ref.virtual_session_id, session.ref.generation, operation)).fetchone()
        if row is None:
            return None
        if row['session_json'] != encoded(session.to_dict()):
            raise ContractError('operation skill catalog owner mismatch')
        revisions = json.loads(row['revisions_json'])
        if row['catalog_digest'] != digest_json([session.to_dict(), operation, revisions, row['epoch']]):
            raise ContractError('operation skill catalog digest mismatch')
    return tuple(repository.get_revision(value) for value in revisions)


def pin_operation_catalog(repository, session, operation, *, route=None):
    """Freeze one effective view without rewriting the generation's base pin."""
    _identifier(operation, 'skill catalog operation')
    base, _ = repository.pin_revision_availability(session)
    def pin(db):
        old = db.execute("SELECT * FROM learning_operation_catalogs WHERE session_id=? AND generation=? AND operation_id=?",
                         (session.ref.virtual_session_id, session.ref.generation, operation)).fetchone()
        if old is not None:
            if old['session_json'] != encoded(session.to_dict()):
                raise ContractError('operation skill catalog owner mismatch')
            revisions = json.loads(old['revisions_json'])
            if old['catalog_digest'] != digest_json([session.to_dict(), operation, revisions, old['epoch']]):
                raise ContractError('operation skill catalog digest mismatch')
            return revisions
        epoch, overrides = head(db, session, route)
        catalog = load_verified_revision_catalog(repository.root, db)
        values = apply_overrides(base, overrides, catalog)
        revisions = [r.revision_digest for r in values]
        db.execute('INSERT INTO learning_operation_catalogs VALUES(?,?,?,?,?,?,?)',
                   (session.ref.virtual_session_id, session.ref.generation, operation,
                    encoded(session.to_dict()), encoded(revisions), epoch,
                    digest_json([session.to_dict(), operation, revisions, epoch])))
        return revisions
    return tuple(repository.get_revision(value) for value in repository._mutate(pin))


class ScopedSkillTrials:
    def __init__(self, experiments):
        self.experiments, self.repository = experiments, experiments.repository
        def initialize(db):
            for statement in TABLES:
                db.execute(statement)
        self.repository._mutate(initialize)

    def current(self, session, *, route=None):
        base, _ = self.repository.pin_revision_availability(session)
        with self.repository._read() as db:
            epoch, overrides = head(db, session, route)
            catalog = load_verified_revision_catalog(self.repository.root, db)
            return epoch, apply_overrides(base, overrides, catalog)

    def enqueue(self, session, operation, trial_id, suite, policy_digest, cancel_generation, *, allow_assets=False, route=None):
        if route is None or ChannelRoute.from_dict(route).session != session:
            raise ContractError('scoped trial requires the exact physical source route')
        trial = self.experiments.get(trial_id)
        candidate = self.repository.get_revision(trial['candidate'])
        with self.repository._read() as db:
            source = db.execute('SELECT kind,receipt_json FROM learning_experiences WHERE ref=? AND owner=?',
                                (trial['source_ref'], session.agent_id)).fetchone()
        facts = json.loads(source['receipt_json'])['receipt'] if source else {}
        if (source is None or source['kind'] != 'task-result' or facts.get('learning_context') != digest_json(context_value(session)[:3])
                or facts.get('operation_id') != operation):
            raise ContractError('scoped trial source belongs to another conversation or task')
        base = pin_operation_catalog(self.repository, session, operation, route=route)
        predecessor = next((r for r in base if r.skill_name == candidate.skill_name), None)
        if candidate.predecessor_digest != (predecessor.revision_digest if predecessor else None):
            raise ContractError('scoped trial predecessor differs from source catalog')
        self.experiments._candidate(candidate, baseline=predecessor, scoped=True, allow_assets=allow_assets)
        cases = [BehaviorCase.from_dict(value) for value in suite['cases']]
        suite_hash = suite_digest(cases)
        def enqueue(db):
            old = db.execute('SELECT * FROM learning_scoped_trials WHERE trial_id=?', (trial_id,)).fetchone()
            epoch, _ = head(db, session, route)
            plan = {'session': session.to_dict(), 'source_operation': operation,
                    'context_id': context_id(session, route), 'route': route, 'candidate': candidate.revision_digest,
                    'base_catalog': [r.revision_digest for r in base], 'epoch': epoch,
                    'suite': suite, 'suite_digest': suite_hash, 'policy_digest': policy_digest,
                    'cancel_generation': cancel_generation, 'allow_assets': allow_assets}
            if old:
                previous = {k: v for k, v in json.loads(old['plan_json']).items() if k not in ('epoch', 'deadline_ms')}
                if previous != {k: v for k, v in plan.items() if k != 'epoch'}:
                    raise ContractError('scoped trial plan changed')
                return self._decode(old)
            db.execute('INSERT INTO learning_scoped_trials(trial_id,context_id,state,plan_json) VALUES(?,?,?,?)',
                       (trial_id, context_id(session, route), 'pending', encoded(plan)))
            return {'trial_id': trial_id, 'state': 'pending', 'plan': plan}
        return self.repository._mutate(enqueue)

    @staticmethod
    def _decode(row):
        value = dict(row)
        value['plan'] = json.loads(value.pop('plan_json'))
        for name in ('outputs', 'assessment', 'previous', 'after'):
            raw = value.pop(name + '_json', None)
            value[name] = json.loads(raw) if raw is not None else None
        return value

    def get(self, trial_id):
        with self.repository._read() as db:
            row = db.execute('SELECT * FROM learning_scoped_trials WHERE trial_id=?', (trial_id,)).fetchone()
            if row is None:
                raise ContractError('scoped trial unavailable')
            return self._decode(row)

    def pending(self, session):
        with self.repository._read() as db:
            return tuple(self._decode(row) for row in db.execute(
                "SELECT * FROM learning_scoped_trials WHERE json_extract(plan_json,'$.session')=? AND state IN ('pending','verified') ORDER BY rowid LIMIT 16",
                (encoded(session.to_dict()),)))

    def active(self, session):
        with self.repository._read() as db:
            return tuple(self._decode(row) for row in db.execute(
                "SELECT t.* FROM learning_scoped_trials t JOIN learning_scoped_heads h ON h.context_id=t.context_id "
                "WHERE json_extract(t.plan_json,'$.session.ref.virtual_session_id')=? "
                "AND json_extract(t.plan_json,'$.session.agent_id')=? AND json_extract(t.plan_json,'$.session.route_id')=? "
                "AND t.state='active' AND EXISTS (SELECT 1 FROM json_each(h.overrides_json) current "
                "WHERE current.value=json_extract(t.plan_json,'$.candidate')) ORDER BY t.rowid",
                (session.ref.virtual_session_id, session.agent_id, session.route_id)))

    def start(self, session, trial_id, policy_digest, *, deadline_ms):
        def start(db):
            row = db.execute('SELECT * FROM learning_scoped_trials WHERE trial_id=?', (trial_id,)).fetchone()
            if row is None or row['state'] != 'pending':
                raise ContractError('scoped comparison already consumed')
            plan = json.loads(row['plan_json'])
            route = plan['route']
            if row['context_id'] != context_id(session, route):
                raise ContractError('scoped comparison route mismatch')
            epoch, _ = head(db, session, route)
            if plan['session'] != session.to_dict() or plan['policy_digest'] != policy_digest or epoch != plan['epoch']:
                raise ContractError('scoped comparison source superseded')
            plan['deadline_ms'] = deadline_ms
            db.execute("UPDATE learning_scoped_trials SET state='evaluating',plan_json=? WHERE trial_id=?", (encoded(plan), trial_id))
            return self._decode(row)
        return self.repository._mutate(start)

    def finish(self, trial_id, baseline, candidate):
        trial = self.get(trial_id)
        cases = tuple(BehaviorCase.from_dict(value) for value in trial['plan']['suite']['cases'])
        assessment = assess_comparison(trial['plan']['candidate'], cases, baseline, candidate)
        state = 'verified' if assessment['accepted'] else ('unknown' if assessment['reason'] == 'unresolved-output' else 'rejected')
        def finish(db):
            changed = db.execute("UPDATE learning_scoped_trials SET state=?,outputs_json=?,assessment_json=? "
                                 "WHERE trial_id=? AND state='evaluating'",
                                 (state, encoded({'baseline': baseline, 'candidate': candidate}), encoded(assessment), trial_id)).rowcount
            if not changed:
                raise ContractError('scoped comparison result stale')
        self.repository._mutate(finish)
        return self.get(trial_id)

    def unknown(self, trial_id, reason):
        self.repository._mutate(lambda db: db.execute(
            "UPDATE learning_scoped_trials SET state='unknown',assessment_json=? WHERE trial_id=? AND state IN ('pending','evaluating','verified')",
            (encoded({'reason': reason, 'replay_allowed': False}), trial_id)))

    def reconcile(self, session):
        self.repository._mutate(lambda db: db.execute(
            "UPDATE learning_scoped_trials SET state='unknown' WHERE json_extract(plan_json,'$.session')=? AND state='evaluating'", (encoded(session.to_dict()),)))

    def adopt(self, session, trial_id, policy_digest, runtime_guard):
        # This generation pin is immutable; mutable overrides are read under the commit lock.
        generation_base, _ = self.repository.pin_revision_availability(session)
        def adopt(db):
            row = db.execute('SELECT * FROM learning_scoped_trials WHERE trial_id=?', (trial_id,)).fetchone()
            if row is None or row['context_id'] != context_id(session, json.loads(row['plan_json'])['route']):
                raise ContractError('scoped adoption owner mismatch')
            if row['state'] == 'active':
                return self._decode(row)  # Durable receipt readback, never another mutation.
            if row['state'] != 'verified':
                raise ContractError('scoped adoption has no verified comparison')
            original = db.execute('SELECT state FROM learning_trials WHERE trial_id=? AND owner=?',
                                  (trial_id, session.agent_id)).fetchone()
            if original is None or original['state'] != 'proposed':
                raise ContractError('scoped source trial state changed')
            value = self._decode(row)
            plan, outputs = value['plan'], value['outputs']
            route = plan['route']
            cases = tuple(BehaviorCase.from_dict(case) for case in plan['suite']['cases'])
            check = assess_comparison(plan['candidate'], cases, outputs['baseline'], outputs['candidate'])
            epoch, overrides = head(db, session, route)
            if (plan['session'] != session.to_dict() or epoch != plan['epoch']
                    or plan['policy_digest'] != policy_digest or check != value['assessment']
                    or not check['accepted'] or check['suite_digest'] != plan['suite_digest']):
                raise ContractError('scoped adoption evidence or epoch changed')
            if (not db.execute('SELECT enabled FROM learning_control WHERE id=1').fetchone()[0]
                    or self.experiments.protection_check() is not True):
                raise ContractError('scoped adoption disabled or unprotected')
            catalog = load_verified_revision_catalog(self.repository.root, db)
            by_digest = {r.revision_digest: r for r in catalog}
            candidate = by_digest[plan['candidate']]
            base = generation_base
            effective = apply_overrides(base, overrides, catalog)
            if [r.revision_digest for r in effective] != plan['base_catalog']:
                raise ContractError('scoped source catalog differs from current effective catalog')
            current = {r.skill_name: r for r in effective}
            predecessor = current.get(candidate.skill_name)
            self.experiments._candidate(candidate, baseline=predecessor, scoped=True, allow_assets=plan['allow_assets'])
            updated = dict(overrides)
            updated[candidate.skill_name] = candidate.revision_digest
            for name, revision in plan['suite'].get('retire', {}).items():
                if (name == candidate.skill_name or name in self.experiments.protected_names
                        or name not in current or current[name].revision_digest != revision):
                    raise ContractError('scoped retirement owner or digest mismatch')
                updated[name] = None
            # Check every surviving dependent; retained bytes and old catalogs remain readable.
            apply_overrides(base, updated, catalog)
            db.execute('INSERT INTO learning_scoped_heads VALUES(?,?,?,?) ON CONFLICT(context_id) '
                       'DO UPDATE SET generation=excluded.generation,overrides_json=excluded.overrides_json',
                       (context_id(session, route), encoded(context_value(session, route)), epoch + 1, encoded(updated)))
            db.execute("UPDATE learning_scoped_trials SET state='active',adoption_generation=?,previous_json=?,after_json=? WHERE trial_id=?",
                       (epoch + 1, encoded(overrides), encoded(updated), trial_id))
            db.execute("UPDATE learning_trials SET state='scoped-active' WHERE trial_id=? AND state='proposed'", (trial_id,))
            return {'state': 'active', 'trial_id': trial_id, 'generation': epoch + 1,
                    'candidate': candidate.revision_digest, 'global_activation': False}
        return runtime_guard(lambda: self.repository._mutate(adopt))

    def rollback(self, session, trial_id, runtime_guard):
        def rollback(db):
            row = db.execute('SELECT * FROM learning_scoped_trials WHERE trial_id=?', (trial_id,)).fetchone()
            if row is None or row['context_id'] != context_id(session, json.loads(row['plan_json'])['route']):
                raise ContractError('scoped rollback owner mismatch')
            value = self._decode(row)
            if row['state'] == 'rolled-back':
                return value
            route = value['plan']['route']
            epoch, overrides = head(db, session, route)
            if (row['state'] != 'active' or epoch != row['adoption_generation']
                    or overrides != value['after']):
                raise ContractError('scoped rollback epoch changed; cannot undo a replacement')
            db.execute('UPDATE learning_scoped_heads SET generation=?,overrides_json=? WHERE context_id=?',
                       (epoch + 1, encoded(value['previous']), context_id(session, route)))
            db.execute("UPDATE learning_scoped_trials SET state='rolled-back' WHERE trial_id=?", (trial_id,))
            db.execute("UPDATE learning_trials SET state='rolled-back' WHERE trial_id=?", (trial_id,))
            return {'state': 'rolled-back', 'generation': epoch + 1, 'external_effects_undone': False}
        return runtime_guard(lambda: self.repository._mutate(rollback))

    def observation(self, trial_id, operation):
        with self.repository._read() as db:
            return db.execute('SELECT 1 FROM learning_scoped_observations WHERE trial_id=? AND operation_id=?',
                              (trial_id, operation)).fetchone() is not None

    def record_observation(self, trial_id, operation, value):
        self.repository._mutate(lambda db: db.execute(
            'INSERT OR IGNORE INTO learning_scoped_observations VALUES(?,?,?)',
            (trial_id, operation, encoded(value))))
