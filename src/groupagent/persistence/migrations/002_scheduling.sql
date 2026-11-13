CREATE TABLE ga_schedules (
    id text PRIMARY KEY,
    group_id text NOT NULL REFERENCES ga_groups(id),
    creator_id text NOT NULL,
    creator_role text NOT NULL CHECK (creator_role IN ('owner','member')),
    request_key text,
    title text NOT NULL CHECK (length(title) > 0),
    goal text NOT NULL CHECK (length(goal) > 0),
    timing jsonb NOT NULL,
    time_zone text NOT NULL,
    starts_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    next_due_at timestamptz,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','paused','ended','cancelled')),
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    policy_revision integer NOT NULL CHECK (policy_revision > 0),
    scope jsonb NOT NULL,
    budget jsonb NOT NULL DEFAULT '{}',
    acceptance_criteria jsonb NOT NULL DEFAULT '[]',
    pause_reason text,
    misfire_policy text NOT NULL DEFAULT 'latest_only' CHECK (misfire_policy = 'latest_only'),
    overlap_policy text NOT NULL DEFAULT 'coalesce' CHECK (overlap_policy = 'coalesce'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (group_id, id),
    UNIQUE (group_id, request_key),
    CHECK (valid_until >= starts_at)
);
CREATE INDEX ga_schedule_due ON ga_schedules(group_id, state, next_due_at);

CREATE TABLE ga_schedule_occurrences (
    id text PRIMARY KEY,
    group_id text NOT NULL,
    schedule_id text NOT NULL,
    scheduled_at timestamptz NOT NULL,
    state text NOT NULL CHECK (state IN
        ('pending','enqueued','running','succeeded','failed','skipped','uncertain')),
    work_id text,
    coalesced_from timestamptz,
    reason text,
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (schedule_id, scheduled_at),
    UNIQUE (work_id),
    FOREIGN KEY (group_id, schedule_id) REFERENCES ga_schedules(group_id, id),
    FOREIGN KEY (group_id, work_id) REFERENCES ga_work_orders(group_id, id),
    CHECK (state NOT IN ('enqueued','running','succeeded','failed','uncertain') OR work_id IS NOT NULL)
);
CREATE INDEX ga_occurrences_schedule_state
    ON ga_schedule_occurrences(group_id, schedule_id, state, scheduled_at);
CREATE UNIQUE INDEX ga_one_pending_occurrence
    ON ga_schedule_occurrences(group_id, schedule_id) WHERE state = 'pending';
