ALTER TABLE ga_team_tasks
    ADD CONSTRAINT ga_task_group_identity UNIQUE(group_id,id),
    ADD COLUMN created_by text,
    ADD COLUMN content_state text NOT NULL DEFAULT 'active' CHECK(content_state IN ('active','forgotten')),
    ADD COLUMN field_sources jsonb NOT NULL DEFAULT '{}' CHECK(jsonb_typeof(field_sources)='object'),
    ADD COLUMN source_event_ids jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(source_event_ids)='array'),
    ADD COLUMN provenance_kind text NOT NULL DEFAULT 'legacy_unattributed' CHECK(provenance_kind IN ('source_bound','independent','legacy_unattributed'));
UPDATE ga_team_tasks t SET created_by=e.actor_id,
    source_event_ids=jsonb_build_array(t.source_event_id),
    field_sources=jsonb_build_object('title',jsonb_build_array(t.source_event_id),'owner_id',jsonb_build_array(t.source_event_id),'status',jsonb_build_array(t.source_event_id),'progress',jsonb_build_array(t.source_event_id),'evidence_refs',jsonb_build_array(t.source_event_id),'due_at',jsonb_build_array(t.source_event_id)),
    provenance_kind='source_bound'
FROM ga_events e WHERE e.group_id=t.group_id AND e.id=t.source_event_id;
CREATE INDEX ga_task_current_sources ON ga_team_tasks USING gin(source_event_ids);
CREATE TABLE ga_task_tombstones (
    group_id text NOT NULL REFERENCES ga_groups(id),
    task_id text NOT NULL,
    deleted_by text NOT NULL,
    deleted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(group_id,task_id),
    FOREIGN KEY(group_id,task_id) REFERENCES ga_team_tasks(group_id,id)
);
