CREATE TABLE ga_groups (
    id text PRIMARY KEY CHECK (length(id) > 0),
    policy_revision integer NOT NULL CHECK (policy_revision > 0)
);

CREATE TABLE ga_events (
    sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    id text PRIMARY KEY,
    group_id text NOT NULL REFERENCES ga_groups(id),
    source_key text NOT NULL UNIQUE,
    actor_id text NOT NULL,
    kind text NOT NULL,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (group_id, id)
);
CREATE INDEX ga_events_group_sequence ON ga_events(group_id, sequence);

CREATE TABLE ga_decisions (
    group_id text NOT NULL,
    event_id text NOT NULL,
    actor_id text NOT NULL,
    policy_revision integer NOT NULL,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (group_id, event_id),
    FOREIGN KEY (group_id, event_id) REFERENCES ga_events(group_id, id)
);

CREATE TABLE ga_cursors (
    group_id text NOT NULL REFERENCES ga_groups(id),
    stream_id text NOT NULL,
    cursor text,
    revision integer NOT NULL DEFAULT 0 CHECK (revision >= 0),
    PRIMARY KEY (group_id, stream_id)
);

CREATE TABLE ga_work_orders (
    id text PRIMARY KEY,
    group_id text NOT NULL REFERENCES ga_groups(id),
    requester_id text NOT NULL,
    request_event_id text NOT NULL,
    goal text NOT NULL CHECK (length(goal) > 0),
    acceptance_criteria jsonb NOT NULL DEFAULT '[]',
    input_versions jsonb NOT NULL DEFAULT '[]',
    scope jsonb NOT NULL DEFAULT '{}',
    budget jsonb NOT NULL DEFAULT '{}',
    required_checks jsonb NOT NULL DEFAULT '["result"]',
    state text NOT NULL CHECK (state IN ('queued','running','waiting_input','waiting_external',
        'validating','delivering','paused','completed','failed','cancelled')),
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    goal_revision integer NOT NULL DEFAULT 1 CHECK (goal_revision > 0),
    policy_revision integer NOT NULL CHECK (policy_revision > 0),
    pause_reason text,
    lease_owner text,
    lease_expires_at timestamptz,
    fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    attempt_no integer NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (group_id, id),
    UNIQUE (group_id, request_event_id),
    FOREIGN KEY (group_id, request_event_id) REFERENCES ga_events(group_id, id)
);
CREATE INDEX ga_work_group_state ON ga_work_orders(group_id, state, created_at);

CREATE TABLE ga_work_revisions (
    group_id text NOT NULL,
    work_id text NOT NULL,
    goal_revision integer NOT NULL,
    body jsonb NOT NULL,
    changed_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (group_id, work_id, goal_revision),
    FOREIGN KEY (group_id, work_id) REFERENCES ga_work_orders(group_id, id)
);

CREATE TABLE ga_execution_attempts (
    id text PRIMARY KEY,
    group_id text NOT NULL,
    work_id text NOT NULL,
    attempt_no integer NOT NULL,
    worker_id text NOT NULL,
    fencing_token bigint NOT NULL,
    goal_revision integer NOT NULL,
    state text NOT NULL CHECK (state IN ('active','released','expired','revoked')),
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    checkpoint_ref text,
    UNIQUE (group_id, work_id, attempt_no),
    UNIQUE (group_id, work_id, fencing_token),
    FOREIGN KEY (group_id, work_id) REFERENCES ga_work_orders(group_id, id)
);
CREATE UNIQUE INDEX ga_one_active_attempt ON ga_execution_attempts(group_id, work_id)
    WHERE state = 'active';

CREATE TABLE ga_work_validations (
    group_id text NOT NULL,
    work_id text NOT NULL,
    goal_revision integer NOT NULL,
    check_key text NOT NULL,
    passed boolean NOT NULL,
    evidence_ref text NOT NULL CHECK (length(evidence_ref) > 0),
    fencing_token bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (group_id, work_id, goal_revision, check_key),
    FOREIGN KEY (group_id, work_id) REFERENCES ga_work_orders(group_id, id)
);

CREATE TABLE ga_deliveries (
    id text PRIMARY KEY,
    group_id text NOT NULL REFERENCES ga_groups(id),
    work_id text,
    goal_revision integer,
    created_by text NOT NULL,
    policy_revision integer NOT NULL CHECK (policy_revision > 0),
    payload_ref text NOT NULL,
    payload_hash text NOT NULL,
    idempotency_key text NOT NULL,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','dispatching','accepted','unknown','rejected')),
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    claim_token bigint NOT NULL DEFAULT 0 CHECK (claim_token >= 0),
    worker_id text,
    claim_expires_at timestamptz,
    platform_message_id text,
    platform_job_id text,
    accepted_at timestamptz,
    evidence_ref text,
    reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (group_id, id),
    UNIQUE (group_id, idempotency_key),
    FOREIGN KEY (group_id, work_id) REFERENCES ga_work_orders(group_id, id),
    CHECK ((work_id IS NULL) = (goal_revision IS NULL)),
    CHECK (state <> 'accepted' OR
        (accepted_at IS NOT NULL AND
         (platform_message_id IS NOT NULL OR platform_job_id IS NOT NULL)))
);
CREATE INDEX ga_pending_deliveries ON ga_deliveries(group_id, created_at) WHERE state = 'pending';
CREATE INDEX ga_dispatching_deliveries ON ga_deliveries(group_id, claim_expires_at)
    WHERE state = 'dispatching';
CREATE INDEX ga_accepted_platform_messages ON ga_deliveries(group_id, platform_message_id)
    WHERE state = 'accepted';

CREATE TABLE ga_team_tasks (
    id text PRIMARY KEY,
    group_id text NOT NULL REFERENCES ga_groups(id),
    title text NOT NULL CHECK (length(title) > 0),
    owner_id text NOT NULL,
    assignment_kind text NOT NULL CHECK (assignment_kind IN ('self_claim','owner_assigned')),
    due_at timestamptz,
    status text NOT NULL DEFAULT 'open'
        CHECK (status IN ('open','in_progress','completed','cancelled')),
    progress text NOT NULL DEFAULT '',
    evidence_refs jsonb NOT NULL DEFAULT '[]',
    source_event_id text,
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (group_id, source_event_id),
    FOREIGN KEY (group_id, source_event_id) REFERENCES ga_events(group_id, id)
);

CREATE TABLE ga_audit_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    group_id text NOT NULL REFERENCES ga_groups(id),
    trace_id text NOT NULL,
    actor_id text NOT NULL,
    policy_revision integer NOT NULL,
    action text NOT NULL,
    resource_ref text,
    details jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ga_audit_group_time ON ga_audit_events(group_id, created_at);
