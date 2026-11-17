CREATE TABLE ga_evolution_versions (
    group_id text NOT NULL REFERENCES ga_groups(id),
    id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('strategy', 'code')),
    name text NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    work_id text NOT NULL,
    goal_revision integer NOT NULL CHECK (goal_revision > 0),
    proposed_by text NOT NULL,
    idempotency_key text NOT NULL,
    source_fingerprints jsonb NOT NULL,
    body jsonb NOT NULL,
    body_sha256 text NOT NULL,
    proposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked boolean NOT NULL DEFAULT false,
    PRIMARY KEY (group_id, id),
    UNIQUE (group_id, kind, name, version),
    UNIQUE (group_id, kind, name, id),
    UNIQUE (group_id, work_id, idempotency_key),
    FOREIGN KEY (group_id, work_id) REFERENCES ga_work_orders(group_id, id)
);

CREATE INDEX ga_evolution_sources ON ga_evolution_versions USING gin(source_fingerprints);

CREATE TABLE ga_evolution_evaluations (
    group_id text NOT NULL,
    version_id text NOT NULL,
    reviewed_by text NOT NULL,
    report_sha256 text NOT NULL,
    report jsonb NOT NULL,
    reviewed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    passed boolean NOT NULL,
    PRIMARY KEY (group_id, version_id),
    FOREIGN KEY (group_id, version_id) REFERENCES ga_evolution_versions(group_id, id)
);

CREATE TABLE ga_evolution_active (
    group_id text NOT NULL,
    kind text NOT NULL,
    name text NOT NULL,
    version_id text NOT NULL,
    PRIMARY KEY (group_id, kind, name),
    FOREIGN KEY (group_id, kind, name, version_id)
       REFERENCES ga_evolution_versions(group_id, kind, name, id)
);

CREATE FUNCTION ga_revoke_evolution_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.memory_state <> 'active' OR OLD.body IS DISTINCT FROM NEW.body THEN
        UPDATE ga_evolution_versions SET body='{}', revoked=true
        WHERE group_id=NEW.group_id AND source_fingerprints ? NEW.id AND NOT revoked;
        DELETE FROM ga_evolution_active a USING ga_evolution_versions v
        WHERE a.group_id=NEW.group_id AND a.group_id=v.group_id AND a.version_id=v.id
        AND v.revoked;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER ga_revoke_evolution_event
AFTER UPDATE OF memory_state, body ON ga_events
FOR EACH ROW EXECUTE FUNCTION ga_revoke_evolution_event();

CREATE FUNCTION ga_revoke_evolution_work() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.scope->'privacy_redacted' = 'true'::jsonb THEN
        UPDATE ga_evolution_versions SET body='{}', revoked=true
        WHERE group_id=NEW.group_id AND work_id=NEW.id AND NOT revoked;
        DELETE FROM ga_evolution_active a USING ga_evolution_versions v
        WHERE a.group_id=NEW.group_id AND a.group_id=v.group_id AND a.version_id=v.id
        AND v.revoked;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER ga_revoke_evolution_work
AFTER UPDATE OF scope ON ga_work_orders
FOR EACH ROW WHEN (NEW.scope->'privacy_redacted' IS DISTINCT FROM OLD.scope->'privacy_redacted')
EXECUTE FUNCTION ga_revoke_evolution_work();
