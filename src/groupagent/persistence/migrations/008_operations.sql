ALTER TABLE ga_team_tasks ALTER COLUMN owner_id DROP NOT NULL;
ALTER TABLE ga_team_tasks DROP CONSTRAINT ga_team_tasks_assignment_kind_check;
ALTER TABLE ga_team_tasks ADD CONSTRAINT ga_team_tasks_assignment_kind_check
    CHECK (assignment_kind IN ('self_claim','owner_assigned','unassigned','legacy_import'));

CREATE SCHEMA IF NOT EXISTS groupagent_recovery;
CREATE TABLE IF NOT EXISTS groupagent_recovery.guard (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    blocked boolean NOT NULL, restore_id text NOT NULL,
    evidence_ref text, updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE ga_ops_backups (
    id text PRIMARY KEY, group_id text NOT NULL REFERENCES ga_groups(id),
    state text NOT NULL CHECK (state IN ('creating','complete','failed','expired')),
    manifest jsonb NOT NULL DEFAULT '{}', error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), expires_at timestamptz NOT NULL,
    created_by text NOT NULL
);
CREATE TABLE ga_ops_restores (
    id text PRIMARY KEY, group_id text NOT NULL REFERENCES ga_groups(id),
    backup_id text NOT NULL, report jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE ga_restore_guards (
    group_id text PRIMARY KEY REFERENCES ga_groups(id), restore_id text NOT NULL,
    blocked boolean NOT NULL DEFAULT true, evidence_ref text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), released_at timestamptz
);
CREATE TABLE ga_legacy_plans (
    id text PRIMARY KEY, group_id text NOT NULL REFERENCES ga_groups(id),
    source_system text NOT NULL, legacy_group_id text NOT NULL, source_sha256 text NOT NULL,
    plan_sha256 text NOT NULL, time_zone text NOT NULL, summary jsonb NOT NULL,
    state text NOT NULL DEFAULT 'planned' CHECK (state IN ('planned','applied')),
    created_by text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), applied_at timestamptz,
    UNIQUE(group_id,source_system,legacy_group_id,source_sha256)
);
CREATE TABLE ga_legacy_map (
    group_id text NOT NULL REFERENCES ga_groups(id), source_system text NOT NULL,
    legacy_group_id text NOT NULL, entity_kind text NOT NULL CHECK(entity_kind IN ('member','team_task')),
    legacy_id text NOT NULL, target_id text NOT NULL, source_row_sha256 text NOT NULL,
    target_revision integer NOT NULL, import_id text NOT NULL REFERENCES ga_legacy_plans(id),
    PRIMARY KEY(group_id,source_system,legacy_group_id,entity_kind,legacy_id)
);
CREATE TABLE ga_legacy_review (
    id text PRIMARY KEY, group_id text NOT NULL REFERENCES ga_groups(id),
    import_id text NOT NULL REFERENCES ga_legacy_plans(id), kind text NOT NULL,
    legacy_key text NOT NULL, body jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','rejected','handled')),
    revision integer NOT NULL DEFAULT 1, evidence_ref text,
    UNIQUE(group_id,import_id,kind,legacy_key)
);
