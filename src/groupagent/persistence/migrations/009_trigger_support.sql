-- Backend facts, not model assertions. Only real state transitions produce facts.
-- No historical backfill: rules cannot manufacture an earlier completion.
CREATE TABLE ga_trigger_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    group_id text NOT NULL REFERENCES ga_groups(id),
    event_type text NOT NULL CHECK(event_type IN ('task_completed','work_completed','work_failed')),
    resource_id text NOT NULL,
    resource_revision integer NOT NULL,
    subject_member_id text,
    source_event_id text,
    occurred_at timestamptz NOT NULL,
    UNIQUE(group_id,event_type,resource_id,resource_revision),
    FOREIGN KEY(group_id,source_event_id) REFERENCES ga_events(group_id,id)
);
CREATE INDEX ga_trigger_events_scan ON ga_trigger_events(group_id,event_type,resource_id,occurred_at,id);
CREATE FUNCTION ga_record_task_trigger_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status AND NEW.status = 'completed' THEN
        INSERT INTO ga_trigger_events(group_id,event_type,resource_id,resource_revision,
            subject_member_id,source_event_id,occurred_at)
        VALUES(NEW.group_id,'task_completed',NEW.id,NEW.revision,NEW.owner_id,
            NEW.source_event_id,NEW.updated_at);
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER ga_task_trigger_event AFTER UPDATE OF status ON ga_team_tasks
    FOR EACH ROW EXECUTE FUNCTION ga_record_task_trigger_event();

CREATE FUNCTION ga_record_work_trigger_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IS DISTINCT FROM NEW.state AND NEW.state IN ('completed','failed') THEN
        INSERT INTO ga_trigger_events(group_id,event_type,resource_id,resource_revision,
            subject_member_id,source_event_id,occurred_at)
        VALUES(NEW.group_id,'work_' || NEW.state,NEW.id,NEW.revision,NEW.requester_id,
            NEW.request_event_id,NEW.updated_at);
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER ga_work_trigger_event AFTER UPDATE OF state ON ga_work_orders
    FOR EACH ROW EXECUTE FUNCTION ga_record_work_trigger_event();
