-- Lineage per source file: one load can take several files (a backlog of
-- weekly drops, or one drop split into _partN files). Records what each file
-- contributed, including rows superseded by a newer file for the same week.
CREATE TABLE IF NOT EXISTS etl.load_files (
    load_id          VARCHAR(60)   NOT NULL,
    source_file      VARCHAR(500)  NOT NULL,
    file_week_end    DATE,
    raw_rows         INTEGER       NOT NULL,
    parsed_rows      INTEGER       NOT NULL,   -- in the window, after parse rejects
    rows_used        INTEGER       NOT NULL,   -- made it into output (newest file for its week)
    superseded_rows  INTEGER       NOT NULL,   -- replaced by a newer file's rows for the same week
    parse_rejects    INTEGER       NOT NULL,
    out_of_window    INTEGER       NOT NULL,
    loaded_at        TIMESTAMP     NOT NULL DEFAULT GETDATE()
);
