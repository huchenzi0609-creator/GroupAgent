"""Trusted selection over synthetic bytes, local receipts and real PostgreSQL."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from docx import Document
from media.conftest import png_fixture

from groupagent.artifacts import ArtifactScope
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role, WorkState
from groupagent.media import MediaSource
from groupagent.memory import MemoryService
from groupagent.runtime.contracts import DeliveryDraft
from groupagent.runtime.dispatch import Dispatcher
from groupagent.runtime.input_selection import canonicalize_input_selection

pytestmark = pytest.mark.postgres


def emit(
    env, *, actor=None, kind=EventKind.TEXT, message_id=None, text="Synthetic selection request"
):
    actor = actor or env.actor
    stamp = datetime.now(UTC)
    value = GroupEvent(
        id=uuid4().hex,
        source_key="selection:" + uuid4().hex,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=kind,
        text=text,
        platform_message_id=message_id,
        occurred_at=stamp,
        received_at=stamp,
    )
    with env.store.transaction(actor) as tx:
        return tx.ingest_event(value, value.id, "1", 0).event


def image(
    env,
    request,
    payload,
    *,
    provenance=None,
    actor=None,
    dependencies=(),
    shared=False,
    lease=None,
    name="synthetic.png",
):
    actor = actor or env.actor
    with env.store.transaction(actor) as tx:
        if lease is None:
            work = tx.create_work("Synthetic image", request.id)
            lease = tx.claim_work(work.id, "selection-fixture", ttl_seconds=300)
        else:
            work = tx.get_work(lease.work_id)
    if provenance is None:
        provenance = MediaSource(
            group_id=actor.group_id,
            work_id=work.id,
            source_event_id=request.id,
            source_message_id=request.id,
            member_id=actor.member_id,
            attachment_id="image:" + request.id,
            declared_kind="image" if name.endswith(".png") else "file",
            origin_kind="research",
            source_url="https://images.example.com/" + request.id + ".png",
        ).model_dump(mode="json")
    else:
        provenance = {**provenance, "work_id": work.id}
    version = env.artifacts.create(
        ArtifactScope(actor.group_id, work.id, actor.member_id),
        name,
        payload,
        provenance=provenance,
    )
    env.registry.register(
        actor,
        lease,
        version,
        source_event_ids=tuple(dependencies),
        validation={"passed": True, "sha256": version.sha256},
    )
    if shared:
        with env.store.transaction(actor) as tx:
            env.registry.mark_shared_in_tx(tx, version.version_id)
    return version, lease


async def accepted_image_ids(env, versions, lease):
    with env.store.transaction(env.actor) as tx:
        work = tx.get_work(lease.work_id)
        validating = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=work.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "synthetic:verified-image")
        tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=validating.revision, lease=lease
        )
        intents = env.state.enqueue_bundle_in_tx(
            tx,
            lease,
            tuple(
                DeliveryDraft("artifact:" + version.version_id, version.sha256, uuid4().hex)
                for version in versions
            ),
        )
    dispatcher = Dispatcher(env.state, env.service, lambda _: env.actor, env.channel, env.registry)
    assert await dispatcher.tick() == len(versions)
    echoes = []
    with env.store.transaction(env.actor) as tx:
        for intent in intents:
            receipt = tx.conn.execute(
                "SELECT platform_message_id FROM ga_delivery_parts WHERE group_id=%s "
                "AND intent_id=%s AND state='accepted'",
                (tx.group_id, intent.id),
            ).fetchone()
            echoes.append(receipt["platform_message_id"])
    return echoes


async def deliver_images(env, versions, lease):
    identifiers = await accepted_image_ids(env, versions, lease)
    return [emit(env, kind=EventKind.IMAGE, message_id=value) for value in identifiers]


async def deliver_image(env, version, lease):
    return (await deliver_images(env, [version], lease))[0]


async def test_originals_imported_copies_and_two_bot_images_reduce_to_two_versions(runtime_env):
    env = runtime_env
    originals, copies, echoes = [], [], []
    copy_requests = []
    for index in range(2):
        origin = emit(env)
        copy_request = emit(env)
        copy_requests.append(copy_request)
        payload = png_fixture("SYNTHETIC FROG " + str(index))
        original, lease = image(env, origin, payload)
        echoes.append(await deliver_image(env, original, lease))
        copy, _ = image(
            env,
            copy_request,
            payload,
            provenance={**original.provenance, "input_version_id": original.version_id},
            dependencies=(origin.id,),
        )
        originals.append(original)
        copies.append(copy)
    request = emit(env)
    selected = [value.version_id for value in [*copies, *originals]]
    with env.store.transaction(env.actor) as tx:
        # Match the caller's first grant check and retain every copy dependency.
        fingerprints = {}
        for version_id in selected:
            fingerprints.update(env.registry.consume_in_tx(tx, version_id)[0])
        versions, channel = canonicalize_input_selection(
            tx, env.registry, request, selected, [echo.id for echo in echoes]
        )
        assert versions == [value.version_id for value in originals]
        assert channel == [] and all(value.id in fingerprints for value in copy_requests)


async def test_receipt_only_selection_maps_to_its_exact_shared_image(runtime_env):
    env = runtime_env
    origin = emit(env)
    original, lease = image(env, origin, png_fixture("EXACT RECEIPT"))
    echo = await deliver_image(env, original, lease)
    unproven = emit(env, kind=EventKind.IMAGE, message_id="unaccepted:human-image")
    request = emit(env)
    with env.store.transaction(env.actor) as tx:
        versions, channel = canonicalize_input_selection(
            tx, env.registry, request, [], [echo.id, unproven.id]
        )
    assert versions == [original.version_id] and channel == [unproven.id]


def test_same_bytes_from_unrelated_authors_remain_separate_inputs(runtime_env):
    env = runtime_env
    other = ActorContext(group_id=env.actor.group_id, member_id="another-author", role=Role.MEMBER)
    payload = png_fixture("SAME BYTES DISTINCT AUTHORS")
    first, _ = image(env, emit(env), payload, shared=True)
    second, _ = image(env, emit(env, actor=other), payload, actor=other, shared=True)
    request = emit(env)
    selected = [first.version_id, second.version_id]
    with env.store.transaction(env.actor) as tx:
        assert canonicalize_input_selection(tx, env.registry, request, selected, []) == (
            selected,
            [],
        )


def test_different_bytes_with_same_origin_never_merge(runtime_env):
    env = runtime_env
    origin = emit(env)
    original, _ = image(env, origin, png_fixture("ORIGINAL"), shared=True)
    changed, _ = image(
        env,
        emit(env),
        png_fixture("DIFFERENT BYTES"),
        provenance={**original.provenance, "input_version_id": original.version_id},
        dependencies=(origin.id,),
    )
    selected = [original.version_id, changed.version_id]
    request = emit(env)
    with env.store.transaction(env.actor) as tx:
        assert canonicalize_input_selection(tx, env.registry, request, selected, []) == (
            selected,
            [],
        )


def test_revoked_copy_dependency_is_checked_before_deduplicating_to_live_original(runtime_env):
    env = runtime_env
    origin, copied_at = emit(env), emit(env)
    payload = png_fixture("REVOKED COPY")
    original, _ = image(env, origin, payload, shared=True)
    copy, _ = image(
        env,
        copied_at,
        payload,
        provenance={**original.provenance, "input_version_id": original.version_id},
        dependencies=(origin.id,),
    )
    request = emit(env)
    MemoryService(env.store).forget(env.actor, event_ids=[copied_at.id])
    with pytest.raises(DomainError) as rejected:
        with env.store.transaction(env.actor) as tx:
            canonicalize_input_selection(
                tx, env.registry, request, [original.version_id, copy.version_id], []
            )
    assert rejected.value.code == "source_unavailable"


async def sent_images(env, *, count=2, same_work=True):
    origin = emit(env)
    originals, leases = [], []
    for index in range(count):
        request = origin if same_work else emit(env)
        provenance = None
        if index and same_work:
            provenance = {
                **originals[0].provenance,
                "attachment_id": "posted-image:" + str(index),
                "source_url": f"https://images.example.com/posted-{index}.png",
            }
        version, lease = image(
            env,
            request,
            png_fixture("POSTED SYNTHETIC SQUIRREL " + str(index)),
            provenance=provenance,
            lease=leases[0] if index and same_work else None,
        )
        originals.append(version)
        leases.append(lease)
    echoes = []
    if same_work:
        echoes = await deliver_images(env, originals, leases[0])
    else:
        for version, lease in zip(originals, leases, strict=True):
            echoes.append(await deliver_image(env, version, lease))
    return originals, echoes


@pytest.mark.parametrize(
    "text",
    [
        "Pot，新建一个独立任务：把已经发到群里的那两张松鼠表情包放进Word文档，写上原始来源链接，直接交付。",
        "把你刚才发的那两张图放Word。",
    ],
)
async def test_sent_image_anaphora_uses_exact_receipts_and_keeps_word_template(runtime_env, text):
    env = runtime_env
    originals, echoes = await sent_images(env)
    unrelated = [
        image(env, emit(env), png_fixture("UNRELATED SEARCH " + str(index)))[0]
        for index in range(2)
    ]
    document = Document()
    document.add_paragraph("Synthetic Word template")
    payload = io.BytesIO()
    document.save(payload)
    template, _ = image(env, emit(env), payload.getvalue(), name="template.docx")
    request = emit(env, text=text)
    with env.store.transaction(env.actor) as tx:
        selected, remaining = canonicalize_input_selection(
            tx,
            env.registry,
            request,
            [value.version_id for value in [*unrelated, template]],
            [value.id for value in echoes],
        )
    assert set(selected) == {value.version_id for value in [*originals, template]}
    assert len(selected) == 3 and remaining == []


@pytest.mark.parametrize("case", ["quantity", "different_work"])
async def test_sent_image_anaphora_requires_exact_quantity_and_one_producing_work(
    runtime_env, case
):
    env = runtime_env
    originals, echoes = await sent_images(
        env, count=1 if case == "quantity" else 2, same_work=case != "different_work"
    )
    request = emit(env, text="把你刚才发的那两张图放Word。")
    with pytest.raises(DomainError) as rejected:
        with env.store.transaction(env.actor) as tx:
            canonicalize_input_selection(
                tx,
                env.registry,
                request,
                [value.version_id for value in originals],
                [value.id for value in echoes],
            )
    assert rejected.value.code == "ambiguous_reference"


async def test_sent_image_anaphora_checks_revoked_unrelated_guess_before_discarding(runtime_env):
    env = runtime_env
    _, echoes = await sent_images(env)
    unrelated_request = emit(env)
    unrelated, _ = image(env, unrelated_request, png_fixture("WITHDRAWN UNRELATED GUESS"))
    request = emit(env, text="把已经发到群里的那两张松鼠表情包放Word，附来源。")
    MemoryService(env.store).forget(env.actor, event_ids=[unrelated_request.id])
    with pytest.raises(DomainError) as rejected:
        with env.store.transaction(env.actor) as tx:
            canonicalize_input_selection(
                tx,
                env.registry,
                request,
                [unrelated.version_id],
                [value.id for value in echoes],
            )
    assert rejected.value.code == "source_unavailable"


def test_sent_image_words_without_channel_references_keep_authorized_versions(runtime_env):
    env = runtime_env
    selected = [
        image(env, emit(env), png_fixture("VERSION ONLY " + str(index)), shared=True)[0].version_id
        for index in range(2)
    ]
    request = emit(env, text="把刚才发的两张图放Word。")
    with env.store.transaction(env.actor) as tx:
        assert canonicalize_input_selection(tx, env.registry, request, selected, []) == (
            selected,
            [],
        )
