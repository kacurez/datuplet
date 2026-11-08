-- Table compaction: hidden per-table system pipelines (sys-compact-*).
ALTER TABLE pipelines ADD COLUMN system BOOLEAN NOT NULL DEFAULT false;
