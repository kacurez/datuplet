-- RFC 027: envelope-free PipelineDoc, JSONB-canonical. DESTRUCTIVE (POC):
-- stored pipelines are discarded and, via runs.pipeline_id ON DELETE CASCADE,
-- run history goes with them. Release notes call this out.
TRUNCATE pipelines CASCADE;
ALTER TABLE pipelines DROP COLUMN yaml;
ALTER TABLE pipelines ADD COLUMN doc jsonb NOT NULL;
ALTER TABLE pipelines ADD COLUMN description text NOT NULL DEFAULT '';
