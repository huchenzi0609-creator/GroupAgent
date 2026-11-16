-- NULL means no monetary limit for that window. A policy row is still required;
-- blocked, price validation and the complete reservation ledger stay intact.
-- Existing positive limits and all historical charges are unchanged.
ALTER TABLE ga_budget_policy ALTER COLUMN daily_limit DROP NOT NULL;
