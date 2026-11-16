-- Additive policy evidence; event and delivery codecs remain unchanged.
ALTER TABLE ga_conversation_policy
    ADD COLUMN quiet_since_sequence bigint CHECK (quiet_since_sequence >= 0);
-- An existing quiet preference predates this boundary. Retire ordinary
-- conversation already ingested at upgrade; work keeps its separate authority.
UPDATE ga_conversation_policy p
SET quiet_since_sequence = (
    SELECT max(e.sequence) FROM ga_events e
    WHERE e.group_id = p.group_id
      AND (p.subject_id = '*' OR e.actor_id = p.subject_id)
)
WHERE p.quiet AND p.quiet_since_sequence IS NULL;
ALTER TABLE ga_participation_log
    ADD COLUMN invocation_root_event_id text,
    ADD CONSTRAINT ga_participation_invocation_root
        FOREIGN KEY (group_id, invocation_root_event_id) REFERENCES ga_events(group_id, id);
CREATE INDEX ga_participation_invocation
    ON ga_participation_log(group_id, invocation_root_event_id)
    WHERE invocation_root_event_id IS NOT NULL;
