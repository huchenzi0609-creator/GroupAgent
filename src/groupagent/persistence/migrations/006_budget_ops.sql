CREATE TABLE ga_price_snapshots (
    group_id text NOT NULL REFERENCES ga_groups(id),
    id text NOT NULL,
    body jsonb NOT NULL,
    PRIMARY KEY (group_id, id)
);
CREATE TABLE ga_budget_policy (
    group_id text PRIMARY KEY REFERENCES ga_groups(id),
    currency text NOT NULL CHECK (currency IN ('CNY', 'USD')),
    daily_limit numeric(24,6) NOT NULL CHECK (daily_limit > 0),
    total_limit numeric(24,6) CHECK (total_limit > 0),
    time_zone text NOT NULL,
    blocked boolean NOT NULL DEFAULT false,
    revision integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE ga_budget_reservations (
    id text PRIMARY KEY,
    group_id text NOT NULL REFERENCES ga_groups(id),
    actor_id text NOT NULL,
    key text NOT NULL,
    operation text NOT NULL,
    price_id text NOT NULL,
    currency text NOT NULL CHECK (currency IN ('CNY', 'USD')),
    budget_day date NOT NULL,
    state text NOT NULL DEFAULT 'reserved' CHECK (state IN ('reserved','inflight','unknown','settled','released')),
    reserved_amount numeric(24,6) NOT NULL CHECK (reserved_amount >= 0),
    actual_amount numeric(24,6) CHECK (actual_amount >= 0),
    estimated_units jsonb NOT NULL,
    actual_units jsonb,
    work_id text,
    source_event_id text,
    policy_revision integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    evidence_ref text,
    reason text,
    overrun boolean NOT NULL DEFAULT false,
    UNIQUE(group_id,key),
    FOREIGN KEY(group_id,price_id) REFERENCES ga_price_snapshots(group_id,id),
    FOREIGN KEY(group_id,work_id) REFERENCES ga_work_orders(group_id,id),
    FOREIGN KEY(group_id,source_event_id) REFERENCES ga_events(group_id,id)
);
CREATE INDEX ga_budget_by_day ON ga_budget_reservations(group_id,budget_day);
