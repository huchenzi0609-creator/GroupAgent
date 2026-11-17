# Durable scheduling foundation

`Scheduler(store)` uses the same trusted `ActorContext` and PostgreSQL transaction boundary as WorkOrder control. It supports `create`, `get`, `list`, `update`, `pause`, `resume`, `cancel`, `occurrences`, `facts`, `due` and the runtime clock methods `next_wake` and `wake`. Changes require the current `expected_revision`. Members control their own rules and owners may control rules within their group. `request_key` makes creation replay idempotent.

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

The conversation accepts typed time proposals. An `after` proposal adds elapsed `seconds` to the verified request's persisted `occurred_at`. A `wall` proposal supplies a `date` and `local_time` in the rule's IANA zone or its own explicit `time_zone`. A skipped wall time requires a new time. A repeated wall time requires `fold: 0` for the first instant or `fold: 1` for the second. The scheduler resolves these proposals to a stored `at` rule and reports a late request against its original instant.

A schedule update can apply a `shift` with signed `seconds` to the selected rule at its checked revision. For daily and weekly rules the change uses local clock time. Weekly shifts across midnight also move the weekday. The rule keeps its identity and existing message or goal when the update supplies timing alone.

Defaults are Asia/Shanghai, at least one hour between interval triggers, ten active rules per group, and thirty days of periodic authorization. One-shot rules allow one catch-up within twenty-four hours of their intended time. Periodic downtime admits only the latest due slot; an expired authorization creates no work. Fixed intervals use elapsed UTC time. Daily and weekly rules use their IANA time zone, skip nonexistent local times, and choose the first occurrence of a repeated wall time.

A rule stores either a future goal or a `PreparedMessage`. A goal opens a WorkOrder with a synthetic decided source at wakeup. A prepared message already has its complete text and goes directly into the shared outbox. Prepared messages require a verified human authorization event. An `explicit_reminder` additionally requires the exact body in its author's request. Personal quiet allows that reminder. Current whole-group quiet holds all scheduled deliveries and records the policy reason.

A due scan atomically records the unique `(schedule_id, scheduled_at)` occurrence, its admission and the next due time. Scanning preserves the creator's authority. A policy revision change pauses new admission until the creator reauthorizes the rule. The runtime uses `wake` in a bounded database transaction and retries contention. The sender uses the same versioned text, stable delivery keys and native receipts for prepared and other outbox records.

The overlap policy keeps one pending occurrence while earlier work or delivery is unresolved. A newer trigger supersedes that pending slot. A prepared delivery held in quiet can be superseded only before any native segment has started. A timing update retires unsubmitted prepared text from the previous rule. Pause stops new admission and holds prepared text at the sender. Cancellation revokes unfinished work and retires proven unsubmitted text. The existing sender reconciles native outcomes for submissions already in progress.

`due` and `wake` return occurrence records created or advanced during the scan. `facts` reads scoped rule timing, recent occurrences and recorded outbox receipts or blocking reasons. Duplicate wakeups, restarts and clock rollback preserve the identity of each admitted slot. `now` comes from the trusted runtime clock. Raw-message expiry can preserve a confirmed standalone grant. Explicit recall or erasure revokes dependent grants and removes their retained content.
