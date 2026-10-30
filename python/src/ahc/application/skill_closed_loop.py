"""Idle, once-only held-out comparison and scoped uptake; no script execution."""

import json

from ahc.domain.child_work import _task_text
from ahc.domain.sessions import ContractError
from ahc.domain.skill_comparison import BehaviorCase, assess_comparison
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.scoped_skill_trials import ScopedSkillTrials, read_operation_catalog, stable_route


def comparison_prompt(repository, revision, case):
    resources = {} if revision is None else {
        asset.path: repository.read_asset(revision.revision_digest, asset.path).decode('utf-8')
        for asset in revision.assets}
    value = {'instruction': revision.body if revision else '', 'resources': resources, 'task': case.prompt}
    prompt = ('Complete the task using the supplied skill instruction and inert resource data. '
              'Use no tools and execute no scripts. Return only the task result as JSON. '
              'No expected answer is supplied.\n' + json.dumps(value, ensure_ascii=False, separators=(',', ':')))
    _task_text(prompt)  # Fidelity first: an overlarge bundle is rejected, never silently truncated.
    return prompt


class LearningClosedLoop:
    def __init__(self, learning, config, calls, policy):
        self.learning, self.queue = learning, learning.handoff
        self.session, self.config, self.calls, self.policy = learning.session, config, calls, policy
        self.policy_digest = digest_json(config)
        self.trials = ScopedSkillTrials(learning.experiments)
        self.trials.reconcile(self.session)
        self.queue.reconcile_followups(self.session)
        self.last = {'state': 'idle', 'global_activation': False}
        self.cleanup_pending = False
        self.catalog_operation = None

    def catalog(self, operation=None):
        operation = operation or self.catalog_operation
        if operation is None:
            sources = self.queue.delivered_sources(self.session)
            if len(sources) != 1:
                raise ContractError('physical catalog requires an exact source operation')
            operation = sources[0]
        route = self.queue.route_for_operation(self.session, operation)
        return self.trials.current(self.session, route=route)[1]

    def enqueue(self, operation, trial):
        suite = self.config['suites'].get(trial['skill_name'])
        if suite is None:
            return {'state': 'proposal-only', 'reason': 'no-owner-held-out-suite'}
        source = self.queue.get(self.session, operation)
        binding = json.loads(source['binding_json'])
        self.catalog_operation = operation
        return self.trials.enqueue(self.session, operation, trial['trial_id'], suite,
                                   self.policy_digest, binding['cancel_generation'],
                                   allow_assets=self.config['allow_assets'],
                                   route=self.queue.route_for_operation(self.session, operation))

    def step(self, *, cancelled):
        if cancelled():
            return None
        try:
            if self.cleanup_pending:
                self.calls.recover(self.session)
                self.queue.reconcile_followups(self.session)
                self.cleanup_pending = False
                return {'state': 'cleanup-only', 'replay_allowed': False}
            observation = self._observe(cancelled)
            if observation is not None:
                self.last = observation
                return observation
            pending = self.trials.pending(self.session)
            if not pending:
                return None
            trial = pending[0]
            plan = trial['plan']
            operation, trial_id = plan['source_operation'], trial['trial_id']
            deadline = plan.get("deadline_ms", self.queue.clock() + self.policy.job_timeout_seconds * 1000)
            def guard(action):
                if cancelled():
                    raise ContractError('closed-loop cancelled')
                return self.queue.guard_effect(self.session, operation, plan['cancel_generation'], action, deadline_ms=deadline)
            if trial['state'] == 'verified':
                result = self.trials.adopt(self.session, trial_id, self.policy_digest, guard)
                self.last = {'state': result['state'], 'trial_id': trial_id, 'global_activation': False}
                return self.last
            guard(lambda: self.trials.start(self.session, trial_id, self.policy_digest, deadline_ms=deadline))
            cases = tuple(BehaviorCase.from_dict(case) for case in plan['suite']['cases'])
            repository = self.learning.experiments.repository
            candidate = repository.get_revision(plan['candidate'])
            baseline = (repository.get_revision(candidate.predecessor_digest)
                        if candidate.predecessor_digest else None)
            # Construct every prompt before the first paid call. Expected answers
            # stay only in host-owned suites and never reach either model.
            prompts = [[comparison_prompt(repository, revision, case) for case in cases]
                       for revision in (baseline, candidate)]
            outputs = [[], []]
            for phase, phase_prompts in enumerate(prompts):
                for number, prompt in enumerate(phase_prompts):
                    if cancelled():
                        raise ContractError('closed-loop priority input or stop')
                    job = 'comparison-' + digest_json([trial_id, phase, number, self.policy_digest])
                    binding = self.queue.reserve_followup(self.session, operation, plan['cancel_generation'],
                                                          self.policy, job, deadline_ms=deadline)
                    if binding is None:
                        raise ContractError('comparison allocation unavailable; no replay')
                    def stopped():
                        if cancelled() or self.queue.clock() >= binding['deadline_ms']:
                            return True
                        try:
                            self.queue.guard_effect(self.session, operation, plan['cancel_generation'], lambda: True,
                                                    deadline_ms=binding['deadline_ms'])
                            return False
                        except Exception:
                            return True
                    output = self.calls.run(self.session, operation, binding, prompt, cancelled=stopped)
                    self.queue.finish_followup(self.session, operation, binding, output)
                    outputs[phase].append(output)
            result = self.trials.finish(trial_id, *outputs)
            if result['state'] == 'verified':
                result = self.trials.adopt(self.session, trial_id, self.policy_digest, guard)
            self.last = {'state': result['state'], 'trial_id': trial_id, 'global_activation': False}
            return self.last
        except Exception as error:
            self.cleanup_pending = callable(getattr(self.calls, 'recover', None))
            if 'trial_id' in locals():
                self.trials.unknown(trial_id, type(error).__name__)
            self.last = {'state': 'deferred' if 'trial_id' not in locals() else 'unknown',
                         'error_kind': type(error).__name__, 'replay_allowed': False,
                         'global_activation': False}
            return self.last

    def _complete_body(self, operation, candidate):
        repository = self.learning.experiments.repository
        with repository._read() as db:
            rows = db.execute('SELECT kind,receipt_json FROM learning_experiences WHERE owner=? '
                              "AND json_extract(receipt_json,'$.receipt.learning_context')=? "
                              "AND json_extract(receipt_json,'$.receipt.operation_id')=? ORDER BY sequence",
                              (self.session.agent_id, self.learning.context_id, operation)).fetchall()
        spans = []
        for row in rows:
            facts = json.loads(row['receipt_json'])['receipt']
            if row['kind'] == 'user-input' and candidate.revision_digest in facts.get('prepared_body_revisions', []):
                return True
            read = facts.get('skill_read', {})
            if (read.get('revision_digest') == candidate.revision_digest and read.get('path') == 'SKILL.md'
                    and read.get('offset_unit') == 'chars' and read.get('total_chars') == len(candidate.body)
                    and type(read.get('offset')) is int):
                end = read.get('next_offset')
                if end is None:
                    end = len(candidate.body)
                if type(end) is int and 0 <= read['offset'] <= end <= len(candidate.body):
                    spans.append((read['offset'], end))
        covered = 0
        for start, end in sorted(spans):
            if start > covered:
                break
            covered = max(covered, end)
        return covered >= len(candidate.body)

    def _observe(self, cancelled):
        for trial in self.trials.active(self.session):
            plan = trial['plan']
            candidate = self.learning.experiments.repository.get_revision(plan['candidate'])
            for operation in self.queue.delivered_sources(self.session):
                if cancelled():
                    return None
                if operation == plan['source_operation'] or self.trials.observation(trial['trial_id'], operation):
                    continue
                evidence = self.queue.evidence(self.session, operation)
                catalog = read_operation_catalog(self.learning.experiments.repository, self.session, operation)
                cases = [BehaviorCase.from_dict(value) for value in plan['suite']['cases']]
                case = next((case for case in cases if case.prompt == evidence['input']), None)
                same_route = stable_route(self.queue.route_for_operation(self.session, operation)) == stable_route(plan['route'])
                known = (same_route and evidence['facts']['accepted'] and not evidence['facts']['unresolved_effects']
                         and catalog is not None and candidate.revision_digest in [r.revision_digest for r in catalog]
                         and self._complete_body(operation, candidate) and case is not None)
                observed = {'state': 'unassessed', 'material_use_verified': False}
                if known:
                    check = assess_comparison(candidate.revision_digest, (case,),
                                              (None,), (evidence['output'],))
                    correct = check['evidence'][0]['candidate_correct']
                    observed = {'state': 'passed' if correct else 'regressed',
                                'body_read_verified': True, 'expected_output_verified': correct,
                                'material_use_verified': False}
                    # Correlated read + task performance is observed behavior,
                    # not a counterfactual proof of causal benefit.
                    if not correct:
                        def guard(action):
                            if cancelled():
                                raise ContractError('scoped rollback cancelled')
                            return self.queue.guard_effect(self.session, operation, plan['cancel_generation'], action)
                        try:
                            observed['rollback'] = self.trials.rollback(self.session, trial['trial_id'], guard)
                        except ContractError as error:
                            observed['rollback'] = {'state': 'blocked', 'error_kind': type(error).__name__,
                                                    'reason': 'source-or-adoption-epoch-changed'}
                            # Keep one observation instead of repeatedly attempting a stale rollback.
                self.trials.record_observation(trial['trial_id'], operation, observed)
                if observed['state'] != 'unassessed':
                    return {'state': 'observed', 'trial_id': trial['trial_id'], 'observation': observed}
        return None
