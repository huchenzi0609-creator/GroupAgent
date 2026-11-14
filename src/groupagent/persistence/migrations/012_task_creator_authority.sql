-- 011 could only see a request author; that is not necessarily the creator.
-- Restrict correction to rows that predate 011. Fresh writes already persist
-- the actual trusted transaction actor and must not be inferred from a source.
UPDATE ga_team_tasks t
SET created_by=(
    SELECT a.actor_id FROM ga_audit_events a
    WHERE a.group_id=t.group_id AND a.resource_ref=t.id
      AND a.action='team_task_created'
    ORDER BY a.created_at,a.id LIMIT 1
)
WHERE t.created_at < (SELECT applied_at FROM ga_schema_migrations WHERE version=11);
