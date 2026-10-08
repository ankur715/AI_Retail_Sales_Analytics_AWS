-- Pipeline Triage Agent (triage/): when a load fails, an LLM agent investigates
-- with read-only tools and records its diagnosis on the load's audit row. The
-- note is a SUGGESTION for a person to review -- nothing acts on it automatically.
-- Redshift adds one column per ALTER TABLE.
ALTER TABLE etl.load_audit ADD COLUMN triage_note VARCHAR(8000);   -- diagnosis + suggested fix (plain English)
ALTER TABLE etl.load_audit ADD COLUMN triage_model VARCHAR(120);   -- Bedrock model id that wrote it
ALTER TABLE etl.load_audit ADD COLUMN triage_tokens INTEGER;       -- input + output tokens used (cost tracking)
