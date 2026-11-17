-- Autonomous observations are evaluations, not copies of conversations.
-- Only a committed, currently public observation can authorize an outbox intent.
CREATE TABLE ga_react_observations (
    group_id text NOT NULL REFERENCES ga_groups(id),
    observation_key text NOT NULL,
    resource_kind text NOT NULL CHECK (resource_kind IN ('work', 'task')),
    resource_id text NOT NULL,
    resource_revision integer NOT NULL,
    source_event_id text NOT NULL,
    source_fingerprints jsonb NOT NULL DEFAULT '{}'::jsonb,
    state text NOT NULL CHECK (state IN ('evaluating', 'deferred', 'silent', 'shadow', 'published')),
    lease_token text,
    lease_until timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    failures integer NOT NULL DEFAULT 0,
    reason_code text,
    proposal_sha256 text,
    delivery_id text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (group_id, observation_key),
    UNIQUE (group_id, delivery_id),
    FOREIGN KEY (group_id, source_event_id) REFERENCES ga_events(group_id, id),
    FOREIGN KEY (group_id, delivery_id) REFERENCES ga_deliveries(group_id, id),
    CHECK ((state = 'published') = (delivery_id IS NOT NULL))
);
