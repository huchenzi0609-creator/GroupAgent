-- Freeze the actual channel text before submission. Legacy parts stay NULL;
-- their existing hashes/receipts remain authoritative and are never rewritten.
ALTER TABLE ga_delivery_parts ADD COLUMN rendered_text text;
