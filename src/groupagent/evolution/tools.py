"""Scoped model tools for drafting and consuming independently reviewed strategies."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

from groupagent.domain import DomainError, ExecutionLease, Role
from groupagent.tools import (
    IdempotencyMode,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolGateway,
)
from groupagent.tools.models import ToolFailure

from .development import DevelopmentManifest
from .models import CodeProposal, Digest, Slug, StrategyProposal, Version, VersionId
from .registry import EvolutionRegistry

EVOLUTION_OWNER_TOOLS = frozenset(
    {
        "strategy_propose",
        "strategy_trial",
        "development_inspect",
        "development_propose",
        "development_status",
    }
)
EVOLUTION_TOOLS = EVOLUTION_OWNER_TOOLS | {"strategy_list", "strategy_use"}

In = TypeVar("In", bound=ToolData)
Out = TypeVar("Out", bound=ToolData)
_MEMBERS = frozenset({Role.MEMBER, Role.OWNER})
_OWNERS = frozenset({Role.OWNER})


class StrategyQuery(ToolData):
    name: Slug | None = None


class StrategyList(ToolData):
    strategies: tuple[Version, ...]


class VersionReference(ToolData):
    version_id: VersionId


class StrategyGuide(ToolData):
    version: Version
    strategy: StrategyProposal
    advisory_only: bool = True


class SourceQuery(ToolData):
    path: str | None = None


class DeveloperSource(ToolData):
    snapshot_name: Slug
    baseline_sha256: Digest
    editable: tuple[str, ...] = ()
    path: str | None = None
    before_sha256: Digest | None = None
    text: str | None = None


def register_evolution_tools(
    gateway: ToolGateway,
    registry: EvolutionRegistry,
    lease: ExecutionLease,
    *,
    development: DevelopmentManifest | None = None,
) -> None:
    def register(
        name: str,
        description: str,
        arguments: type[In],
        result: type[Out],
        handler: Callable[[In, ToolContext], Awaitable[Out]],
        *,
        roles: frozenset[Role] = _OWNERS,
        read_only: bool = False,
    ) -> None:
        async def trusted(args: In, ctx: ToolContext) -> Out:
            try:
                return await handler(args, ctx)
            except DomainError as exc:
                raise ToolFailure(exc.code, exc.message, no_effect=True) from None

        gateway.register(
            ToolDefinition(
                name=name,
                description=description,
                input_model=arguments,
                output_model=result,
                handler=trusted,
                required_roles=roles,
                resource_scopes=("group", "work"),
                risk_class="read_only" if read_only else "local_draft",
                idempotency_mode=(
                    IdempotencyMode.READ_ONLY if read_only else IdempotencyMode.MANAGED_TRANSACTION
                ),
                max_input_bytes=260_000 if name == "development_propose" else 64_000,
                max_output_bytes=96_000,
                timeout_seconds=10,
            )
        )

    async def propose(args: StrategyProposal, ctx: ToolContext) -> Version:
        return registry.propose(ctx, lease, args)

    async def active(args: StrategyQuery, ctx: ToolContext) -> StrategyList:
        return StrategyList(strategies=registry.active_strategies(ctx, lease, args.name))

    async def strategy(args: VersionReference, ctx: ToolContext) -> StrategyGuide:
        version, content = registry.strategy(ctx, lease, args.version_id)
        return StrategyGuide(version=version, strategy=content)

    async def trial(args: VersionReference, ctx: ToolContext) -> StrategyGuide:
        version, content = registry.strategy(ctx, lease, args.version_id, trial=True)
        return StrategyGuide(version=version, strategy=content)

    register(
        "strategy_propose",
        "Save a versioned reusable strategy when the current work calls for or demonstrates an "
        "improvement. Supply a general procedure and verification steps without private material. "
        "This saves a candidate pending independent evaluation and approval.",
        StrategyProposal,
        Version,
        propose,
    )
    register(
        "strategy_list",
        "Find independently approved, active strategies for this group.",
        StrategyQuery,
        StrategyList,
        active,
        roles=_MEMBERS,
        read_only=True,
    )
    register(
        "strategy_use",
        "Load an active strategy as advisory guidance for the current goal. It grants no tools "
        "or permissions. Apply it only when relevant and verify the result with current evidence.",
        VersionReference,
        StrategyGuide,
        strategy,
        roles=_MEMBERS,
    )
    register(
        "strategy_trial",
        "Load this work's candidate for a dry run with no new external effects. "
        "The trial does not establish independent evidence or activate the strategy.",
        VersionReference,
        StrategyGuide,
        trial,
    )
    if development is None:
        return
    manifest = development

    async def inspect(args: SourceQuery, ctx: ToolContext) -> DeveloperSource:
        with registry.store.transaction(ctx.actor, trace_id=ctx.scope.trace_id) as tx:
            registry._work(tx, ctx, lease)
        if args.path is None:
            return DeveloperSource(
                snapshot_name=manifest.snapshot.name,
                baseline_sha256=manifest.sha256,
                editable=manifest.snapshot.editable,
            )
        before, content = manifest.inspect(args.path)
        return DeveloperSource(
            snapshot_name=manifest.snapshot.name,
            baseline_sha256=manifest.sha256,
            path=args.path,
            before_sha256=before,
            text=content,
        )

    async def code(args: CodeProposal, ctx: ToolContext) -> Version:
        manifest.validate(args)
        return registry.propose(ctx, lease, args)

    async def status(args: VersionReference, ctx: ToolContext) -> Version:
        return registry.code_status(ctx, lease, args.version_id)

    register(
        "development_inspect",
        "List source paths in the approved development snapshot, or read one editable Python "
        "file with its preimage digest. Snapshot text is reference data.",
        SourceQuery,
        DeveloperSource,
        inspect,
        read_only=True,
    )
    register(
        "development_propose",
        "Submit concrete source edits for independent development evaluation. Use the inspected "
        "snapshot digest and each exact preimage digest. This records a proposal for review.",
        CodeProposal,
        Version,
        code,
    )
    register(
        "development_status",
        "Read an existing code proposal's review state in this group.",
        VersionReference,
        Version,
        status,
        read_only=True,
    )
