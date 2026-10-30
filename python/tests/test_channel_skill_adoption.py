"""Exact target revisions and shared readiness preserve continuation authority."""

import json
import sys

import pytest
from skill_test_support import repository, revision, session, skill_root
from test_channel_continuation import configured, transition

from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_continuation import (
    continuation_blockers,
    read_skill_continuation_receipt,
)
from ahc.infrastructure.skill_evaluator import evaluate_revision
from ahc.infrastructure.skill_repository import SkillRepository

__all__ = ["repository", "skill_root"]


def active(repository, name, key):
    candidate = revision(name=name)
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    assert report.verdict == "accept"
    evaluation = repository.save_evaluation(report)
    repository.activate(
        skill_name=name,
        candidate_digest=candidate.revision_digest,
        expected_active_digest=None,
        evaluation_digest=evaluation,
        input_digest=report.input_digest,
        operation_key=key,
    )
    return candidate.revision_digest


def test_exact_pin_rejects_catalog_drift_and_never_rewrites_old_pin(repository):
    first = active(repository, "first", "one")
    owner = session(1)
    repository.pin_revision_availability(owner, expected_revisions=(first,))
    second = active(repository, "second", "two")
    with pytest.raises(ContractError, match="active skill catalog"):
        repository.pin_revision_availability(session(2), expected_revisions=(first,))
    values, existed = repository.pin_revision_availability(owner, expected_revisions=(first,))
    assert existed and [item.revision_digest for item in values] == [first]
    with pytest.raises(ContractError, match="successor skill pin"):
        repository.pin_revision_availability(owner, expected_revisions=(first, second))


def test_continuation_reserves_exact_successor_pin_and_retry_is_idempotent(tmp_path, repository):
    digest = active(repository, "first", "one")
    store, old, route, _, lock = configured(tmp_path)
    kwargs = dict(skill_repository=repository, expected_skill_revisions=(digest,))
    with pytest.raises(RuntimeError, match="precommit"):
        transition(store, lock, old, [route], fail_before_commit=True, **kwargs)
    assert store.read_current("session") == old
    assert read_skill_continuation_receipt(store, old) is None
    pinned, existed = repository.pin_revision_availability(old.session.successor())
    assert existed and pinned[0].revision_digest == digest
    assert transition(store, lock, old, [route], **kwargs)["state"] == "applied"
    assert transition(store, lock, old, [route], **kwargs)["state"] == "already-applied"
    assert store.read_current("session").session.ref.generation == 2
    receipt = read_skill_continuation_receipt(store, old)
    assert receipt["skill_target"]["revisions"] == [digest]
    with pytest.raises(ContractError, match="skill target must be retained"):
        transition(store, lock, old, [route])
    other = SkillRepository.create(repository.root.parent / "other-skills")
    assert active(other, "first", "other-one") == digest
    with pytest.raises(ContractError, match="skill target must be retained"):
        transition(
            store, lock, old, [route], skill_repository=other, expected_skill_revisions=(digest,)
        )


def test_wrong_target_cannot_advance_runtime(tmp_path, repository):
    active(repository, "first", "one")
    store, old, route, consumer, lock = configured(tmp_path)
    with pytest.raises(ContractError, match="active skill catalog"):
        transition(
            store,
            lock,
            old,
            [route],
            skill_repository=repository,
            expected_skill_revisions=("d" * 64,),
        )
    assert store.read_current("session") == old and consumer.offset() == 2


def test_shared_preflight_lists_all_store_blockers(tmp_path):
    store, old, route, _, lock = configured(tmp_path)

    def block(connection):
        connection.execute("CREATE TABLE backend_custody (state TEXT)")
        connection.execute("INSERT INTO backend_custody VALUES ('held')")
        connection.execute("CREATE TABLE backend_creation_intents (state TEXT)")
        connection.execute("INSERT INTO backend_creation_intents VALUES ('pending')")

    store._mutate(block)
    with store._read() as connection:
        assert continuation_blockers(connection, old) == (
            "backend_custody",
            "backend_creation_intents",
        )
    with pytest.raises(ContractError, match="backend_custody"):
        transition(store, lock, old, [route])


def test_targeted_retry_binds_retained_quarantine_bytes(tmp_path, repository):
    from test_channel_continuation import held_fixture

    from ahc.domain.skill_values import canonical_json

    digest = active(repository, "first", "retained-quarantine")
    store, old, route, _, lock, _ = held_fixture(tmp_path)
    kwargs = dict(expected_offset=3, skill_repository=repository,
                  expected_skill_revisions=(digest,))
    first = transition(store, lock, old, [route], **kwargs)
    assert first["retained_telegram_holds"]["count"] == 1
    assert transition(store, lock, old, [route], **kwargs)["state"] == "already-applied"
    def corrupt(connection):
        connection.execute("UPDATE runtime_telegram_holds SET update_json=?", (
            canonical_json({"update_id": 2, "message": {"changed": True}}),))
    store._mutate(corrupt)
    with pytest.raises(ContractError, match="quarantine receipt changed"):
        transition(store, lock, old, [route], **kwargs)
    assert store.read_current("session").session.ref.generation == 2


@pytest.mark.skipif(sys.platform != "linux", reason="native foreground configuration")
def test_native_cli_emits_bound_successor_config_without_starting_effects(
    tmp_path, repository, capsys
):
    from test_foreground_config_native import native_config

    from ahc.cli.channel_continuation import main
    from ahc.cli.foreground_config_native import parse_trusted_foreground_config

    digest = active(repository, "first", "cli-one")
    store, old, route, consumer, lock = configured(tmp_path)
    value = native_config()
    value.update(runtime_root=str(store.root), session=old.session.to_dict())
    value["channels"] = [
        {"route": route.to_dict(), "token_env": "ISOLATED_UNUSED_TOKEN", "enabled": False}
    ]
    value["telegram_polling"] = {
        "enabled": False,
        "account_id": "123",
        "token_env": "ISOLATED_UNUSED_TOKEN",
        "exclusive_consumer": True,
        "interval_seconds": 1,
    }
    original = parse_trusted_foreground_config(value)
    config, snapshot, revisions, output = (
        tmp_path / name
        for name in ("config.json", "expected.json", "revisions.json", "successor.json")
    )
    config.write_text(json.dumps(value))
    snapshot.write_text(json.dumps(old.to_dict()))
    revisions.write_text(json.dumps([digest]))
    assert (
        main(
            [
                "--config",
                str(config),
                "--expected-config-digest",
                original.configuration_digest,
                "--expected-snapshot",
                str(snapshot),
                "--expected-offset",
                "2",
                "--supervisor-lock",
                str(lock),
                "--output-config",
                str(output),
                "--target-skill-root",
                str(repository.root),
                "--expected-skill-revisions",
                str(revisions),
                "--retire-native-custody",
            ]
        )
        == 0
    )
    receipt = json.loads(capsys.readouterr().out)
    selected = parse_trusted_foreground_config(json.loads(output.read_text()))
    assert receipt["state"] == "applied"
    assert selected.configuration_digest == receipt["configuration_digest"]
    assert selected.session == old.session.successor()
    assert selected.skill_root == str(repository.root)
    assert receipt["skill_target"]["revisions"] == [digest]
    assert selected.to_dict()["telegram_polling"]["enabled"] is False
    assert consumer.routes[0].session == old.session  # Retained old consumer is not rewritten.
