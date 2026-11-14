CREATE TABLE ga_process_leases (
    group_id text NOT NULL REFERENCES ga_groups(id),
    name text NOT NULL,
    owner_id text NOT NULL,
    fencing_token bigint NOT NULL DEFAULT 1,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY(group_id,name)
);
CREATE TABLE ga_runtime_checkpoints (
    group_id text NOT NULL,
    work_id text NOT NULL,
    attempt_id text NOT NULL,
    goal_revision integer NOT NULL,
    sequence bigint NOT NULL,
    body jsonb NOT NULL,
    source_event_ids text[] NOT NULL DEFAULT '{}',
    source_fingerprints jsonb NOT NULL DEFAULT '{}',
    invalidated boolean NOT NULL DEFAULT false,
    saved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(group_id,work_id,attempt_id),
    FOREIGN KEY(group_id,work_id) REFERENCES ga_work_orders(group_id,id)
);
CREATE TABLE ga_runtime_events (
    group_id text NOT NULL,
    work_id text NOT NULL,
    attempt_id text NOT NULL,
    sequence bigint NOT NULL,
    kind text NOT NULL,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(group_id,work_id,attempt_id,sequence),
    FOREIGN KEY(group_id,work_id) REFERENCES ga_work_orders(group_id,id)
);
CREATE TABLE ga_artifact_catalog (
    group_id text NOT NULL,
    version_id text NOT NULL,
    file_id text NOT NULL,
    work_id text NOT NULL,
    created_by text NOT NULL,
    name text NOT NULL,
    format text NOT NULL,
    sha256 text NOT NULL,
    byte_length bigint NOT NULL,
    body jsonb NOT NULL,
    validation jsonb,
    source_event_ids text[] NOT NULL DEFAULT '{}',
    source_fingerprints jsonb NOT NULL DEFAULT '{}',
    available boolean NOT NULL DEFAULT true,
    shared boolean NOT NULL DEFAULT false,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(group_id,version_id),
    FOREIGN KEY(group_id,work_id) REFERENCES ga_work_orders(group_id,id)
);
CREATE INDEX ga_artifact_sources ON ga_artifact_catalog USING gin(source_event_ids);
CREATE TABLE ga_work_outputs (
    group_id text NOT NULL,
    work_id text NOT NULL,
    goal_revision integer NOT NULL,
    version_id text NOT NULL,
    validation jsonb,
    purpose text NOT NULL DEFAULT 'result',
    PRIMARY KEY(group_id,work_id,goal_revision,version_id),
    FOREIGN KEY(group_id,work_id) REFERENCES ga_work_orders(group_id,id),
    FOREIGN KEY(group_id,version_id) REFERENCES ga_artifact_catalog(group_id,version_id)
);
CREATE TABLE ga_delivery_bundles (
    group_id text NOT NULL,
    bundle_id text NOT NULL,
    intent_id text NOT NULL,
    position integer NOT NULL CHECK(position>=0),
    PRIMARY KEY(group_id,intent_id),
    UNIQUE(group_id,bundle_id,position),
    FOREIGN KEY(group_id,intent_id) REFERENCES ga_deliveries(group_id,id)
);
CREATE TABLE ga_delivery_parts (
    group_id text NOT NULL,
    intent_id text NOT NULL,
    position integer NOT NULL CHECK(position>=0),
    idempotency_key text NOT NULL,
    payload_hash text NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','submitted','accepted','unknown','rejected')),
    platform_message_id text,
    platform_job_id text,
    reason text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(group_id,intent_id,position),
    UNIQUE(group_id,idempotency_key),
    FOREIGN KEY(group_id,intent_id) REFERENCES ga_deliveries(group_id,id)
);
CREATE INDEX ga_part_receipt ON ga_delivery_parts(group_id,platform_message_id);
CREATE TABLE ga_runtime_alerts (
    group_id text NOT NULL REFERENCES ga_groups(id),
    fingerprint text NOT NULL,
    code text NOT NULL,
    state text NOT NULL DEFAULT 'open',
    occurrences integer NOT NULL DEFAULT 1,
    first_seen timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(group_id,fingerprint)
);
CREATE TABLE ga_conversation_policy (
    group_id text NOT NULL REFERENCES ga_groups(id),
    subject_id text NOT NULL,
    mode text NOT NULL DEFAULT 'natural' CHECK(mode IN ('natural','on_request','paused','disabled')),
    quiet boolean NOT NULL DEFAULT false,
    quiet_until timestamptz,
    revision integer NOT NULL DEFAULT 1,
    PRIMARY KEY(group_id,subject_id)
);
CREATE TABLE ga_participation_log (
    group_id text NOT NULL,
    event_id text NOT NULL,
    proactive boolean NOT NULL,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(group_id,event_id),
    FOREIGN KEY(group_id,event_id) REFERENCES ga_events(group_id,id)
);
CREATE TABLE ga_runtime_configuration (
    group_id text PRIMARY KEY REFERENCES ga_groups(id),
    policy_revision integer NOT NULL,
    sha256 text NOT NULL
);
CREATE TABLE ga_media_inputs (
    group_id text NOT NULL,
    event_id text NOT NULL,
    attachment_id text NOT NULL,
    body jsonb NOT NULL,
    source_fingerprints jsonb NOT NULL DEFAULT '{}',
    available boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(group_id,event_id,attachment_id),
    FOREIGN KEY(group_id,event_id) REFERENCES ga_events(group_id,id)
);
