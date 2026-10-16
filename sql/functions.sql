-- The extension's functions (ch_table_engines.md §2.4).
SET client_min_messages = warning;
CREATE TABLE fn (a int) USING mergetree WITH (order_by = 'a');
INSERT INTO fn SELECT generate_series(1, 10);
INSERT INTO fn SELECT generate_series(11, 20);
SELECT name, rows, bytes > 0 AS has_bytes, min_rowid <= max_rowid AS ids, level FROM ch.parts('fn');
SELECT ch.table_size('fn') = (SELECT sum(bytes) FROM ch.parts('fn')) AS size;
-- pg_relation_size() measures the main fork: the pages store's data, nothing of the files store's
SELECT (pg_relation_size('fn') > 0) = (current_setting('ch.default_storage') = 'pages') AS main_fork,
	   ch.table_size('fn') > 0 AS parts;
-- the library's threads: its four idle executors' and the backend's, after every call
SELECT threads FROM ch.engine_info();
SELECT ch.parts('pg_class');
DROP TABLE fn;
