CREATE TABLE ga_topic_origins (
    group_id text NOT NULL,
    topic_id text NOT NULL,
    source_event_ids text[] NOT NULL,
    PRIMARY KEY(group_id,topic_id),
    FOREIGN KEY(group_id,topic_id) REFERENCES ga_memory_topics(group_id,id)
);
CREATE TABLE ga_event_retry (
    group_id text NOT NULL,
    event_id text NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    blocked boolean NOT NULL DEFAULT false,
    code text,
    PRIMARY KEY(group_id,event_id),
    FOREIGN KEY(group_id,event_id) REFERENCES ga_events(group_id,id)
);
CREATE TABLE ga_trigger_rules (
    id text PRIMARY KEY,
    group_id text NOT NULL REFERENCES ga_groups(id),
    creator_id text NOT NULL,
    creator_role text NOT NULL,
    request_key text NOT NULL,
    event_type text NOT NULL CHECK(event_type IN ('task_completed','work_completed','work_failed','attachment_received')),
    resource_id text NOT NULL,
    goal text NOT NULL,
    max_occurrences integer NOT NULL CHECK(max_occurrences BETWEEN 1 AND 10),
    count integer NOT NULL DEFAULT 0,
    expires_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'active' CHECK(state IN ('active','paused','cancelled','ended')),
    revision integer NOT NULL DEFAULT 1,
    policy_revision integer NOT NULL,
    derived_depth integer NOT NULL DEFAULT 0 CHECK(derived_depth BETWEEN 0 AND 1),
    scope jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(group_id,id),
    UNIQUE(group_id,request_key)
);
CREATE TABLE ga_trigger_occurrences (
    group_id text NOT NULL,
    rule_id text NOT NULL,
    event_key text NOT NULL,
    work_id text,
    state text NOT NULL CHECK(state IN ('enqueued','skipped')),
    reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(group_id,rule_id,event_key),
    FOREIGN KEY(group_id,rule_id) REFERENCES ga_trigger_rules(group_id,id),
    FOREIGN KEY(group_id,work_id) REFERENCES ga_work_orders(group_id,id)
);
CREATE TABLE ga_progress_notices (
    group_id text NOT NULL,
    work_id text NOT NULL,
    notice_key text NOT NULL,
    last_sent_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    intent_id text NOT NULL,
    PRIMARY KEY(group_id,work_id,notice_key),
    FOREIGN KEY(group_id,work_id) REFERENCES ga_work_orders(group_id,id),
    FOREIGN KEY(group_id,intent_id) REFERENCES ga_deliveries(group_id,id)
);
