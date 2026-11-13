# Durable scheduling foundation

`Scheduler(store)` uses the same trusted `ActorContext` and PostgreSQL transaction boundary as WorkOrder control. It supports `create`, `get`, `list`, `update`, `pause`, `resume`, `cancel`, `occurrences` and `due`. Changes require the current `expected_revision`; members control their own rules and owners may control rules within their group. `request_key` makes creation replay idempotent.

```python
from datetime import datetime, UTC
from groupagent.scheduling import Scheduler

scheduler = Scheduler(store)
rule = scheduler.create(
    actor,
    "Evening summary",
    "Summarize the authorized project status",
    {"kind": "daily", "local_time": "20:00"},
    request_key="source-event:summary-rule",
)
occurrences = scheduler.due(actor, datetime.now(UTC))
```

Accepted timing objects are `AtTiming(at=<aware timestamp>)`, `EveryTiming(interval_seconds=3600, start_at=<optional aware timestamp>)`, `DailyTiming(local_time="20:00")`, and `WeeklyTiming(local_time="20:00", weekdays=[0, 4])`. Weekdays use Monday=0. Equivalent JSON dictionaries include a `kind` value of `at`, `every`, `daily`, or `weekly`. Cron, RRULE, monthly and event triggers return `unsupported_schedule`.

Defaults are Asia/Shanghai, at least one hour between interval triggers, ten active rules per group, and thirty days of periodic authorization. One-shot rules allow one catch-up within twenty-four hours of their intended time. Periodic downtime admits only the latest due slot; an expired authorization creates no work. Fixed intervals use elapsed UTC time. Daily and weekly rules use their IANA time zone, skip nonexistent local times, and choose the first occurrence of a repeated wall time.

A due scan atomically writes the unique `(schedule_id, scheduled_at)` occurrence, a synthetic source event, a decided event record, its WorkOrder and the next due time. An owner scanning a member's rule preserves the original creator as the WorkOrder requester. Policy revision changes pause admission until the creator reauthorizes the rule. Synthetic events are already decided and do not re-enter conversation classification.

The overlap policy keeps one pending occurrence while a previous work is active or has an uncertain delivery. A newer trigger supersedes that pending slot with an auditable skipped record. Once prior work ends, a later scan admits the remaining pending slot. Pause stops pending admission and pauses admitted work, revoking its lease; resume only requeues work paused by the schedule. Cancellation cancels unfinished admitted work and retains delivery history. Existing accepted deliveries remain recorded.

`due` returns occurrence records created or advanced during the scan; `occurrences` reads durable history. Scans also synchronize occurrence status with WorkOrder and delivery state. Duplicate wakeups, process restarts, and clock rollback do not create a second work for an acknowledged slot. `now` is a trusted control clock, exposed for deterministic tests; it must not come from a model.

This is a callable scheduling foundation, not a background daemon or a natural-language time parser. It does not send messages, reserve money, verify live channel delivery, or supervise an OS worker. Scheduled work passes through the same execution lease, per-tool policy checks, validation and outbox as other WorkOrders. A control service must invoke `due` periodically and continue enforcing current identity/configuration and execution budgets. Already-running external operations retain their ordinary reconciliation requirements.
