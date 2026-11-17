ALTER TABLE ga_schedules ADD COLUMN execution_kind text NOT NULL DEFAULT 'goal'
    CHECK (execution_kind IN ('goal','notification','explicit_reminder'));

ALTER TABLE ga_schedule_occurrences ADD COLUMN delivery_id text;
ALTER TABLE ga_schedule_occurrences ADD COLUMN delivery_purpose text;
ALTER TABLE ga_schedule_occurrences ADD COLUMN delivery_sources text[] NOT NULL DEFAULT '{}';
ALTER TABLE ga_schedule_occurrences
    DROP CONSTRAINT ga_schedule_occurrences_check,
    ADD CONSTRAINT ga_occurrence_delivery FOREIGN KEY (group_id, delivery_id)
        REFERENCES ga_deliveries(group_id, id),
    ADD CONSTRAINT ga_one_occurrence_per_delivery UNIQUE (delivery_id),
    ADD CONSTRAINT ga_occurrence_admission CHECK (
        (work_id IS NULL OR delivery_id IS NULL)
        AND ((delivery_id IS NULL AND delivery_purpose IS NULL AND cardinality(delivery_sources)=0) OR
            (delivery_id IS NOT NULL AND delivery_purpose IS NOT NULL
                AND delivery_purpose IN ('notification','explicit_reminder')))
        AND (state NOT IN ('enqueued','running','succeeded','failed','uncertain')
            OR work_id IS NOT NULL OR delivery_id IS NOT NULL)
    );

CREATE INDEX ga_schedule_delivery_sources ON ga_schedule_occurrences USING gin(delivery_sources)
    WHERE delivery_id IS NOT NULL;
