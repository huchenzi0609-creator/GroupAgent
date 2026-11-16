-- A request may have one early acknowledgement and one final text reply.
-- The existing UNIQUE(group_id, idempotency_key) keeps each fixed slot unique;
-- the source-event foreign key remains the authority and privacy dependency.
ALTER TABLE ga_deliveries DROP CONSTRAINT ga_one_reply_per_request;
ALTER TABLE ga_deliveries DROP CONSTRAINT ga_reply_text_only;
ALTER TABLE ga_deliveries ADD CONSTRAINT ga_reply_text_only CHECK (
    request_event_id IS NULL OR (
        work_id IS NULL AND goal_revision IS NULL AND left(payload_ref, 5) = 'text:'
        AND idempotency_key IN (
            'reply:' || request_event_id || ':1',
            'early_ack:' || request_event_id || ':1'
        )
    )
);
