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
	library			text NOT NULL,
	-- the columns ClickHouse keeps as their plain types, not Nullable: NOT
	-- NULL where the storage was made, and since (§2.5, VC11)
	plain			int2[] NOT NULL
) USING heap;
CREATE UNIQUE INDEX tables_storage ON ch.tables (tablespace, relfilenumber, backend);

-- A part's row: its visibility is the part's (§2.6, MVCC rules 1-4).
CREATE TABLE ch.parts (
	tablespace		oid NOT NULL,
	relfilenumber	oid NOT NULL,
	backend			integer NOT NULL,
	name			text COLLATE "C" NOT NULL,	-- "C" as catalog.c's scan keys compare it,
											-- whatever the database's default: its
											-- index in that order
	rows			bigint NOT NULL,
	bytes			bigint NOT NULL,
	min_rowid		bigint NOT NULL,
	max_rowid		bigint NOT NULL,
	level			integer NOT NULL,
	marks			integer NOT NULL,	-- its granules
	manifest		bytea,				-- the pages store's: each file's pages; NULL in the files store
	created			timestamptz NOT NULL,	-- when it was written: its age for the merge selector
	summary			bytea,				-- its columns' bytes and statistics, the engine library's: the
										-- planner's without loading the part; NULL where none was kept
	text_checked	boolean NOT NULL DEFAULT false	-- its text values each their encoding's whole as
										-- written: rows through PostgreSQL's input functions, or
										-- a merge or fold of such parts alone (pg_accel's F5)
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
-- And a dropped storage's directory, once VACUUM has pruned its ch.tables
-- row, where the end of a prepared transaction left it (CH6, store.c).
ALTER TABLE ch.tables SET (autovacuum_vacuum_threshold = 0, autovacuum_vacuum_scale_factor = 0);
ALTER TABLE ch.tables_unlogged SET (autovacuum_vacuum_threshold = 0, autovacuum_vacuum_scale_factor = 0);
-- A part's summary, read at every planning, stays in its row, compressed, where
-- the row's other values leave it room: no TOAST fetch for it.
ALTER TABLE ch.parts ALTER COLUMN summary SET STORAGE MAIN;
ALTER TABLE ch.parts_unlogged ALTER COLUMN summary SET STORAGE MAIN;
CREATE UNLOGGED TABLE ch.tombstones_unlogged (LIKE ch.tombstones) USING heap;
CREATE INDEX tombstones_unlogged_storage_rowid ON ch.tombstones_unlogged (tablespace, relfilenumber, backend, rowid);

REVOKE ALL ON ch.tables, ch.parts, ch.tombstones, ch.tables_unlogged, ch.parts_unlogged, ch.tombstones_unlogged FROM PUBLIC;
GRANT SELECT ON ch.tables, ch.parts, ch.tombstones, ch.tables_unlogged, ch.parts_unlogged, ch.tombstones_unlogged TO PUBLIC;

-- On the Cloudberry port (CH6), each node's catalogs are its own: the
-- storages, parts and deletions of the node's share of each table, written by
-- the node itself.  gp_sql gives a table an extension's script makes the
-- policy "replicated", under which the coordinator reads one segment's rows,
-- gpexpand copies one segment's onto every other and ALTER EXTENSION ...
-- UPDATE empties the coordinator's; without gp_core's "gp" label a table is
-- the node's own, as the port's modules' catalogs are.  Every node runs the
-- script, gp_core with it on every node or on none.
DO $$
BEGIN
	IF current_setting('gp.role', true) IS NOT NULL THEN
		SECURITY LABEL FOR gp ON TABLE ch.tables IS NULL;
		SECURITY LABEL FOR gp ON TABLE ch.parts IS NULL;
		SECURITY LABEL FOR gp ON TABLE ch.tombstones IS NULL;
		SECURITY LABEL FOR gp ON TABLE ch.tables_unlogged IS NULL;
		SECURITY LABEL FOR gp ON TABLE ch.parts_unlogged IS NULL;
		SECURITY LABEL FOR gp ON TABLE ch.tombstones_unlogged IS NULL;
	END IF;
END
$$;

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

-- Tables of files PostgreSQL does not own, read by ClickHouse's own readers
-- and never written (ch_table_engines.md §2.17): WITH (location = '...'), a
-- file, a glob or a directory, and settings, ClickHouse's for the format.
CREATE FUNCTION ch.parquet_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_parquet_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD parquet TYPE TABLE HANDLER ch.parquet_handler;
COMMENT ON ACCESS METHOD parquet IS 'Parquet files, read by ClickHouse''s Parquet reader';

CREATE FUNCTION ch.arrow_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_arrow_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD arrow TYPE TABLE HANDLER ch.arrow_handler;
COMMENT ON ACCESS METHOD arrow IS 'Arrow IPC files, read by ClickHouse''s Arrow IPC reader';

CREATE FUNCTION ch.iceberg_handler(internal) RETURNS table_am_handler
	AS 'MODULE_PATHNAME', 'ch_iceberg_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER ch.iceberg_handler;
COMMENT ON ACCESS METHOD iceberg IS 'Iceberg tables, read by ClickHouse''s Iceberg code';

-- What a table's parts are, as the snapshot sees them.
CREATE FUNCTION ch.parts(rel regclass,
	OUT name text, OUT rows bigint, OUT bytes bigint,
	OUT min_rowid bigint, OUT max_rowid bigint, OUT level integer)
	RETURNS SETOF record
	AS 'MODULE_PATHNAME', 'ch_parts' LANGUAGE C STRICT STABLE;

-- The ClickHouse CREATE TABLE the engine library compiles for a table.
CREATE FUNCTION ch.definition(rel regclass) RETURNS text
	AS 'MODULE_PATHNAME', 'ch_definition' LANGUAGE C STRICT STABLE;

-- The ClickHouse settings an external table, or one of its own files, is read
-- with (ch_table_engines.md §3, VC26): the extension's defaults -- Parquet's
-- dictionary filter limited for a table of its own files -- then the table's.
CREATE FUNCTION ch.read_settings(rel regclass) RETURNS text
	AS 'MODULE_PATHNAME', 'ch_read_settings' LANGUAGE C STRICT VOLATILE;

-- The bytes of a table's parts the snapshot sees.
CREATE FUNCTION ch.table_size(rel regclass) RETURNS bigint
	AS 'MODULE_PATHNAME', 'ch_table_size' LANGUAGE C STRICT STABLE;

-- The engine library: ClickHouse's release, the process's threads, and the
-- functions and aggregate functions it has registered so far.
CREATE FUNCTION ch.engine_info(OUT version text, OUT threads integer, OUT functions integer)
	AS 'MODULE_PATHNAME', 'ch_engine_info' LANGUAGE C STRICT VOLATILE;

-- What this session's fetches of rows by their TIDs read (ch_table_engines.md
-- §3, VC16): the rows read from a part, the columns decoded for them, those
-- rows read of every column, the rows taken from a granule kept from a fetch
-- before, the row ids found among offsets located before, and the rows read
-- where they lie (VC34).
CREATE FUNCTION ch.fetch_stats(OUT fetches bigint, OUT columns bigint, OUT whole bigint,
							   OUT kept bigint, OUT located bigint, OUT pointed bigint)
	AS 'MODULE_PATHNAME', 'ch_fetch_stats' LANGUAGE C STRICT VOLATILE;

-- The units a parallel scan makes of parts of these granules (ch_table_engines.md
-- VC36): each the granules left to claim over 34, within a least of 64K rows --
-- a Parquet file's reader's of its whole footer, a table of files not its own
-- (VC39), a million -- and a most of a million; each unit's part, from 1, its
-- first granule and its granules.
CREATE FUNCTION ch.parallel_units(part_granules int4[], granule_rows int8 DEFAULT 8192,
								  parquet bool DEFAULT false,
								  OUT part int4, OUT first_granule int4, OUT granules int4)
	RETURNS SETOF record
	AS 'MODULE_PATHNAME', 'ch_parallel_units' LANGUAGE C STRICT IMMUTABLE;

-- MergeTree parts' streams kept in shared memory for a GPU's units (ch_table_engines.md
-- §3, VC29): the entries kept now, NULL where none can be -- the extension not
-- preloaded, or ch.device_streams 0 -- this session's runs of granules found
-- among them and described by the engine library, and the parts the library
-- has loaded in the session's process.
CREATE FUNCTION ch.device_streams(OUT kept integer, OUT found bigint, OUT described bigint,
								  OUT parts_loaded bigint)
	AS 'MODULE_PATHNAME', 'ch_device_streams_info' LANGUAGE C STRICT VOLATILE;

-- MergeTree parts' granules a filter keeps, kept in shared memory (ch_table_engines.md
-- §3, VC31): the entries kept now, NULL where none can be -- the extension not
-- preloaded, or ch.part_selections 0 -- and this session's answers found among
-- them, kept, and compared with the engine library's (CHTE_SPEC_CHECK).
CREATE FUNCTION ch.part_selections(OUT kept integer, OUT found bigint, OUT stored bigint,
								   OUT checked bigint)
	AS 'MODULE_PATHNAME', 'ch_part_selections_info' LANGUAGE C STRICT VOLATILE;

-- The footers of Parquet and Arrow files the session's process has read from
-- them (ch_table_engines.md §3, VC25), each file's at its first use in the
-- process: a table of its own files is planned from its parts' summaries, its
-- files' footers read by its units' readers alone.
CREATE FUNCTION ch.footers_read() RETURNS bigint
	AS 'MODULE_PATHNAME', 'ch_footers_read' LANGUAGE C VOLATILE;

-- The readers of Parquet files' units the session's process has given a
-- footer of their own row groups alone (pg_accel's §3.32 F13, VC39), the
-- others' column chunks left out, where each parsed its file's whole footer.
CREATE FUNCTION ch.footers_pruned() RETURNS bigint
	AS 'MODULE_PATHNAME', 'ch_footers_pruned' LANGUAGE C VOLATILE;

-- The columns whose units' ranges the session's process has decoded from parts'
-- summaries (pg_accel's §3.32 F11, VC37): a summary keeps each column's
-- ranges apart, decoded as a filter first asks for them, where every
-- column's were decoded as the part was listed.
CREATE FUNCTION ch.ranges_decoded() RETURNS bigint
	AS 'MODULE_PATHNAME', 'ch_ranges_decoded' LANGUAGE C VOLATILE;

-- The rows the session's process has read alone of the uncompressed record
-- batches of Arrow files (ch_table_engines.md §3, VC27): a fetch's rows by
-- their TIDs read from their batch's buffers at those rows, not the batch's
-- columns whole.
CREATE FUNCTION ch.rows_read_alone() RETURNS bigint
	AS 'MODULE_PATHNAME', 'ch_rows_read_alone' LANGUAGE C VOLATILE;

-- How the processes that started the engine have exited since the server
-- started (ch_table_engines.md §3, VC23): by _exit() once PostgreSQL's own
-- exits had run (ch.fast_exit), or through the engine's teardown and the
-- static destructors; NULLs where the extension is not preloaded.
CREATE FUNCTION ch.engine_exits(OUT fast bigint, OUT torn_down bigint)
	AS 'MODULE_PATHNAME', 'ch_engine_exits' LANGUAGE C STRICT VOLATILE;

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

-- The files store's sweep of the database: the directories of storages no
-- ch.tables row names any more, which a crash leaves, or the end of a
-- prepared transaction -- on the Cloudberry port's segments, of every
-- distributed DDL statement (CH6).  The merge workers' passes end with it.
CREATE FUNCTION ch.remove_orphans() RETURNS integer
	AS 'MODULE_PATHNAME', 'ch_remove_orphans_database' LANGUAGE C STRICT VOLATILE;

-- The columns of the first file a location names, as a column list to start
-- a CREATE TABLE ... USING am from (§2.17); a column no table of files reads,
-- a comment.  Its caller may read the server's files (pg_read_server_files).
CREATE FUNCTION ch.external_columns(am text, location text, settings text DEFAULT NULL) RETURNS text
	AS 'MODULE_PATHNAME', 'ch_external_columns' LANGUAGE C CALLED ON NULL INPUT VOLATILE;

-- The files a table of files reads in this statement, as it lists them: each
-- one's bytes, rows, units -- Parquet's row groups, Arrow's record batches, an
-- Iceberg table's data file one -- and its first row's id.
CREATE FUNCTION ch.external_files(rel regclass,
	OUT path text, OUT bytes bigint, OUT rows bigint, OUT units integer, OUT first_row bigint)
	RETURNS SETOF record
	AS 'MODULE_PATHNAME', 'ch_external_files' LANGUAGE C STRICT VOLATILE;

-- A table of its own files (§2.18, CH8): the parts the caller's snapshot sees,
-- each one's file and its layout as read back from it -- its units, Parquet's
-- row groups or Arrow's record batches, and their rows; its codec; whether a
-- Parquet file has a page index and bloom filters.
CREATE FUNCTION ch.own_files(rel regclass,
	OUT part text, OUT file text, OUT bytes bigint, OUT rows bigint, OUT min_rowid bigint, OUT max_rowid bigint,
	OUT units integer, OUT min_unit_rows bigint, OUT max_unit_rows bigint, OUT codec text, OUT page_index boolean,
	OUT bloom_filters boolean)
	RETURNS SETOF record
	AS 'MODULE_PATHNAME', 'ch_own_files' LANGUAGE C STRICT VOLATILE;

-- A query's rows written to a file by ClickHouse's own writer of the format,
-- 'Parquet' or 'Arrow', as the tests make their tables' files (§2.17): columns,
-- ClickHouse's "name type, ..." of the file -- by default the query's columns,
-- Nullable -- and settings, ClickHouse's for the format.  Its caller may write
-- the server's files (pg_write_server_files).  Returns the file's bytes.
CREATE FUNCTION ch.write_file(format text, path text, query text, columns text DEFAULT NULL,
							  settings text DEFAULT NULL) RETURNS bigint
	AS 'MODULE_PATHNAME', 'ch_write_file' LANGUAGE C CALLED ON NULL INPUT VOLATILE;
REVOKE ALL ON FUNCTION ch.write_file(text, text, text, text, text) FROM PUBLIC;

-- An Iceberg table at a location written by ClickHouse's own Iceberg writers,
-- as the tests make theirs (§2.17), a snapshot an action: 'create' a table of
-- columns, ClickHouse's "name type, ...", partitioned by a spec in the query's
-- place, as ClickHouse's PARTITION BY names one; 'append' the query's rows; 'position
-- deletes' the rows at the query's file_path and pos; 'equality deletes' the
-- rows holding the query's values in its columns; 'alter' its schema changed
-- by a command of ClickHouse's ALTER, the query's place -- and settings,
-- ClickHouse's.  Its caller may write the server's files.  Returns the bytes
-- of the files added.
CREATE FUNCTION ch.write_iceberg(location text, action text, query text DEFAULT NULL, columns text DEFAULT NULL,
								 settings text DEFAULT NULL) RETURNS bigint
	AS 'MODULE_PATHNAME', 'ch_write_iceberg' LANGUAGE C CALLED ON NULL INPUT VOLATILE;
REVOKE ALL ON FUNCTION ch.write_iceberg(text, text, text, text, text) FROM PUBLIC;
