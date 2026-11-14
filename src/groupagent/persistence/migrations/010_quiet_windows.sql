ALTER TABLE ga_conversation_policy ADD COLUMN quiet_hours text[] NOT NULL DEFAULT '{}';
