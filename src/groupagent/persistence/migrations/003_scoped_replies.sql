ALTER TABLE ga_deliveries ADD COLUMN request_event_id text;
ALTER TABLE ga_deliveries ADD CONSTRAINT ga_reply_source_event
    FOREIGN KEY (group_id, request_event_id) REFERENCES ga_events(group_id, id);
ALTER TABLE ga_deliveries ADD CONSTRAINT ga_one_reply_per_request
    UNIQUE (group_id, request_event_id);
ALTER TABLE ga_deliveries ADD CONSTRAINT ga_reply_text_only CHECK (
    request_event_id IS NULL OR (
        work_id IS NULL AND goal_revision IS NULL AND left(payload_ref, 5) = 'text:'
        AND idempotency_key = 'reply:' || request_event_id || ':1'
    )
);
