/* ch_table_engines--0.1.sql (ch_table_engines.md §2.4, §2.6) */

\echo Use "CREATE EXTENSION ch_table_engines" to load this file. \quit

CREATE SCHEMA ch;

-- The catalogs are heap's, whatever default_table_access_method says.

-- A table's storage: one row per relfilenumber the extension made, with its row
-- id and block counters (§2.6, "Row ids").  The counters move in place and
-- never back, as a sequence's do.
CREATE TABLE ch.tables (
	tablespace		oid NOT NULL,
	relfilenumber	oid NOT NULL,
	backend			integer NOT NULL,	-- a temporary table's backend, else -1
	relid			oid NOT NULL,
	engine			text NOT NULL,
	storage			text NOT NULL,
	next_rowid		bigint NOT NULL,
	next_block		bigint NOT NULL,
	library			text NOT NULL
) USING heap;
CREATE UNIQUE INDEX tables_storage ON ch.tables (tablespace, relfilenumber, backend);

-- A part's row: its visibility is the part's (§2.6, MVCC rules 1-4).
CREATE TABLE ch.parts (
	tablespace		oid NOT NULL,
	relfilenumber	oid NOT NULL,
	backend			integer NOT NULL,
	name			text NOT NULL,
	rows			bigint NOT NULL,
	bytes			bigint NOT NULL,
	min_rowid		bigint NOT NULL,
	max_rowid		bigint NOT NULL,
	level			integer NOT NULL,
	marks			integer NOT NULL,	-- its granules
	manifest		bytea,				-- the pages store's: each file's pages; NULL in the files store
	created			timestamptz NOT NULL	-- when it was written: its age for the merge selector
) USING heap;
CREATE UNIQUE INDEX parts_storage_name ON ch.parts (tablespace, relfilenumber, backend, name);

-- A row's deletion, an update's, or a lock on it, by the row's id (§2.10): a
-- row of a visible part is visible when no deletion or update the snapshot sees
-- names it; a lock holds while its transaction runs.  An update's row names its
-- new version's row id.  Rows of a statement are inserted under its command id.
CREATE TABLE ch.tombstones (
	tablespace		oid NOT NULL,
	relfilenumber	oid NOT NULL,
	backend			integer NOT NULL,
	rowid			bigint NOT NULL,
	kind			"char" NOT NULL,	-- 'd' deleted, 'u' updated, 'l' locked
	successor		bigint				-- 'u': the new version's row id
) USING heap;
CREATE INDEX tombstones_storage_rowid ON ch.tombstones (tablespace, relfilenumber, backend, rowid);

-- The same for UNLOGGED and temporary tables, which a crash resets with them.
CREATE UNLOGGED TABLE ch.tables_unlogged (LIKE ch.tables) USING heap;
CREATE UNIQUE INDEX tables_unlogged_storage ON ch.tables_unlogged (tablespace, relfilenumber, backend);
CREATE UNLOGGED TABLE ch.parts_unlogged (LIKE ch.parts) USING heap;
CREATE UNIQUE INDEX parts_unlogged_storage_name ON ch.parts_unlogged (tablespace, relfilenumber, backend, name);
-- A part's storage is freed once VACUUM has pruned its row (§2.11): a merge
-- leaves a few dead rows, which autovacuum takes at once, as it would not
-- under the default thresholds.
ALTER TABLE ch.parts SET (autovacuum_vacuum_threshold = 0, autovacuum_vacuum_scale_factor = 0);
ALTER TABLE ch.parts_unlogged SET (autovacuum_vacuum_threshold = 0, autovacuum_vacuum_scale_factor = 0);
CREATE UNLOGGED TABLE ch.tombstones_unlogged (LIKE ch.tombstones) USING heap;
CREATE INDEX tombstones_unlogged_storage_rowid ON ch.tombstones_unlogged (tablespace, relfilenumber, backend, rowid);

REVOKE ALL ON ch.tables, ch.parts, ch.tombstones, ch.tables_unlogged, ch.parts_unlogged, ch.tombstones_unlogged FROM PUBLIC;
GRANT SELECT ON ch.tables, ch.parts, ch.tombstones, ch.tables_unlogged, ch.parts_unlogged, ch.tombstones_unlogged TO PUBLIC;

CREATE FUNCTION ch.mergetree_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_mergetree_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD mergetree TYPE TABLE HANDLER ch.mergetree_handler;
COMMENT ON ACCESS METHOD mergetree IS 'ClickHouse''s MergeTree';

CREATE FUNCTION ch.replacingmergetree_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_replacingmergetree_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD replacingmergetree TYPE TABLE HANDLER ch.replacingmergetree_handler;
COMMENT ON ACCESS METHOD replacingmergetree IS 'ClickHouse''s ReplacingMergeTree';

CREATE FUNCTION ch.summingmergetree_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_summingmergetree_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD summingmergetree TYPE TABLE HANDLER ch.summingmergetree_handler;
COMMENT ON ACCESS METHOD summingmergetree IS 'ClickHouse''s SummingMergeTree';

CREATE FUNCTION ch.aggregatingmergetree_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_aggregatingmergetree_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD aggregatingmergetree TYPE TABLE HANDLER ch.aggregatingmergetree_handler;
COMMENT ON ACCESS METHOD aggregatingmergetree IS 'ClickHouse''s AggregatingMergeTree';

CREATE FUNCTION ch.collapsingmergetree_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_collapsingmergetree_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD collapsingmergetree TYPE TABLE HANDLER ch.collapsingmergetree_handler;
COMMENT ON ACCESS METHOD collapsingmergetree IS 'ClickHouse''s CollapsingMergeTree';

CREATE FUNCTION ch.versionedcollapsingmergetree_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_versionedcollapsingmergetree_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD versionedcollapsingmergetree TYPE TABLE HANDLER ch.versionedcollapsingmergetree_handler;
COMMENT ON ACCESS METHOD versionedcollapsingmergetree IS 'ClickHouse''s VersionedCollapsingMergeTree';

CREATE FUNCTION ch.coalescingmergetree_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_coalescingmergetree_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD coalescingmergetree TYPE TABLE HANDLER ch.coalescingmergetree_handler;
COMMENT ON ACCESS METHOD coalescingmergetree IS 'ClickHouse''s CoalescingMergeTree';

-- What a table's parts are, as the snapshot sees them.
CREATE FUNCTION ch.parts(rel regclass,
	OUT name text, OUT rows bigint, OUT bytes bigint,
	OUT min_rowid bigint, OUT max_rowid bigint, OUT level integer)
	RETURNS SETOF record
	AS 'MODULE_PATHNAME', 'ch_parts' LANGUAGE C STRICT STABLE;

-- The ClickHouse CREATE TABLE the engine library compiles for a table.
CREATE FUNCTION ch.definition(rel regclass) RETURNS text
	AS 'MODULE_PATHNAME', 'ch_definition' LANGUAGE C STRICT STABLE;

-- The bytes of a table's parts the snapshot sees.
CREATE FUNCTION ch.table_size(rel regclass) RETURNS bigint
	AS 'MODULE_PATHNAME', 'ch_table_size' LANGUAGE C STRICT STABLE;

-- The engine library: ClickHouse's release, the process's threads, and the
-- functions and aggregate functions it has registered so far.
CREATE FUNCTION ch.engine_info(OUT version text, OUT threads integer, OUT functions integer)
	AS 'MODULE_PATHNAME', 'ch_engine_info' LANGUAGE C STRICT VOLATILE;

-- The deletions every snapshot sees, folded into their parts (§2.10): each
-- part holding their rows written again without them, their tombstones
-- gone.  Returns the rows folded.
CREATE FUNCTION ch.fold(rel regclass) RETURNS bigint
	AS 'MODULE_PATHNAME', 'ch_fold' LANGUAGE C STRICT VOLATILE;

-- The table's parts merged in this session (§2.11), as OPTIMIZE TABLE merges
-- them: those ClickHouse's merge selector chooses, or with final every part
-- into one, their deleted rows folded out.  Returns how many parts merged.
CREATE FUNCTION ch.optimize(rel regclass, final boolean DEFAULT false) RETURNS integer
	AS 'MODULE_PATHNAME', 'ch_optimize' LANGUAGE C STRICT VOLATILE;

-- The files store's sweep of one table: the directories no part row names,
-- left by a crash or by a prepared transaction's end (§2.9).  VACUUM runs it
-- too, when no writer holds the table.
CREATE FUNCTION ch.remove_orphans(rel regclass) RETURNS integer
	AS 'MODULE_PATHNAME', 'ch_remove_orphans' LANGUAGE C STRICT VOLATILE;
