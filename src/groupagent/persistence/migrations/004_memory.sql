ALTER TABLE ga_events ADD COLUMN memory_state text NOT NULL DEFAULT 'active'
    CHECK (memory_state IN ('active','forgotten','expired','recalled'));
ALTER TABLE ga_events ADD COLUMN occurred_at timestamptz;
UPDATE ga_events SET occurred_at = (body->>'occurred_at')::timestamptz;
ALTER TABLE ga_events ALTER COLUMN occurred_at SET NOT NULL;
CREATE INDEX ga_events_memory_time ON ga_events(group_id, memory_state, occurred_at, sequence);
CREATE TABLE ga_source_tombstones (
    group_id text NOT NULL REFERENCES ga_groups(id), source_key text NOT NULL,
    event_id text, platform_message_id text, channel_account_id text,
    reason text NOT NULL CHECK (reason IN ('forgotten','expired','recalled')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (group_id, source_key)
);
CREATE INDEX ga_tombstone_event ON ga_source_tombstones(group_id,event_id);
CREATE INDEX ga_tombstone_platform ON ga_source_tombstones(group_id,channel_account_id,platform_message_id);

CREATE TABLE ga_members (
    group_id text NOT NULL REFERENCES ga_groups(id), member_id text NOT NULL,
    room_name text, confirmed_name text, confirmed_by text,
    membership_state text NOT NULL DEFAULT 'unknown'
        CHECK (membership_state IN ('unknown','present','left')),
    membership_event_id text,
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (group_id, member_id),
    FOREIGN KEY (group_id, membership_event_id) REFERENCES ga_events(group_id, id)
);
CREATE TABLE ga_roster_runs (
    group_id text NOT NULL REFERENCES ga_groups(id), run_key text NOT NULL,
    state text NOT NULL CHECK (state IN ('fetching','complete','failed')),
    token text NOT NULL, result jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
    PRIMARY KEY (group_id, run_key)
);
CREATE TABLE ga_memory_epochs (
    group_id text PRIMARY KEY REFERENCES ga_groups(id), epoch bigint NOT NULL DEFAULT 0
);
INSERT INTO ga_memory_epochs(group_id) SELECT id FROM ga_groups;

CREATE TABLE ga_memory_topics (
    id text PRIMARY KEY, group_id text NOT NULL REFERENCES ga_groups(id), title text NOT NULL,
    visibility text NOT NULL CHECK (visibility IN ('group','participants')),
    participant_ids jsonb NOT NULL DEFAULT '[]', created_by text NOT NULL,
    revision integer NOT NULL DEFAULT 1, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (group_id,id)
);
CREATE TABLE ga_topic_events (
    group_id text NOT NULL, topic_id text NOT NULL, event_id text NOT NULL,
    linked_by text NOT NULL, evidence_refs jsonb NOT NULL,
    PRIMARY KEY (group_id,topic_id,event_id),
    FOREIGN KEY (group_id,topic_id) REFERENCES ga_memory_topics(group_id,id),
    FOREIGN KEY (group_id,event_id) REFERENCES ga_events(group_id,id)
);

CREATE TABLE ga_memory_facts (
    id text PRIMARY KEY, group_id text NOT NULL REFERENCES ga_groups(id), topic_id text,
    subject_id text, kind text NOT NULL CHECK (kind IN ('fact','decision')),
    predicate text NOT NULL, value jsonb NOT NULL, source_event_ids jsonb NOT NULL,
    created_by text NOT NULL, request_key text,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','invalidated','forgotten')),
    revision integer NOT NULL DEFAULT 1, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (group_id,id), UNIQUE (group_id,request_key),
    FOREIGN KEY (group_id,topic_id) REFERENCES ga_memory_topics(group_id,id)
);
CREATE TABLE ga_fact_revisions (
    group_id text NOT NULL, fact_id text NOT NULL, revision integer NOT NULL,
    predicate text NOT NULL, value jsonb NOT NULL, source_event_ids jsonb NOT NULL,
    state text NOT NULL, changed_by text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (group_id,fact_id,revision),
    FOREIGN KEY (group_id,fact_id) REFERENCES ga_memory_facts(group_id,id)
);
CREATE TABLE ga_fact_sources (
    group_id text NOT NULL, fact_id text NOT NULL, event_id text NOT NULL,
    PRIMARY KEY (group_id,fact_id,event_id),
    FOREIGN KEY (group_id,fact_id) REFERENCES ga_memory_facts(group_id,id),
    FOREIGN KEY (group_id,event_id) REFERENCES ga_events(group_id,id)
);

CREATE TABLE ga_memory_checkpoints (
    group_id text NOT NULL REFERENCES ga_groups(id), topic_key text NOT NULL,
    through_sequence bigint NOT NULL DEFAULT 0, revision integer NOT NULL DEFAULT 1,
    lease_token text, lease_until timestamptz,
    PRIMARY KEY (group_id,topic_key)
);
CREATE TABLE ga_memory_summaries (
    id text PRIMARY KEY, group_id text NOT NULL REFERENCES ga_groups(id), topic_id text,
    text text NOT NULL, source_event_ids jsonb NOT NULL,
    first_sequence bigint NOT NULL, last_sequence bigint NOT NULL,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','invalidated','forgotten')),
    incomplete boolean NOT NULL DEFAULT true CHECK (incomplete),
    provider text NOT NULL, model text NOT NULL, usage jsonb NOT NULL DEFAULT '{}',
    created_by text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (group_id,id),
    FOREIGN KEY (group_id,topic_id) REFERENCES ga_memory_topics(group_id,id)
);
CREATE TABLE ga_summary_sources (
    group_id text NOT NULL, summary_id text NOT NULL, event_id text NOT NULL,
    PRIMARY KEY (group_id,summary_id,event_id),
    FOREIGN KEY (group_id,summary_id) REFERENCES ga_memory_summaries(group_id,id),
    FOREIGN KEY (group_id,event_id) REFERENCES ga_events(group_id,id)
);
CREATE TABLE ga_memory_cleanup (
    id text PRIMARY KEY, group_id text NOT NULL REFERENCES ga_groups(id),
    event_id text, work_id text, resource_kind text NOT NULL
        CHECK (resource_kind IN ('event_attachments','work_artifacts','backup_tombstone')),
    resource_refs jsonb NOT NULL DEFAULT '[]', state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','complete')),
    not_before timestamptz NOT NULL DEFAULT clock_timestamp(),
    revision integer NOT NULL DEFAULT 1, evidence_ref text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (group_id,event_id) REFERENCES ga_events(group_id,id),
    FOREIGN KEY (group_id,work_id) REFERENCES ga_work_orders(group_id,id)
);
CREATE INDEX ga_cleanup_pending ON ga_memory_cleanup(group_id,not_before) WHERE state='pending';
CREATE INDEX ga_fact_lookup ON ga_memory_facts(group_id,state,subject_id);
CREATE INDEX ga_summary_lookup ON ga_memory_summaries(group_id,state,topic_id,last_sequence);
