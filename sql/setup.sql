-- The extension, and what the other tests share.
CREATE EXTENSION ch_table_engines;
SELECT amname, amtype FROM pg_am WHERE amhandler::text LIKE 'ch.%' ORDER BY amname;
-- the engine library loads on first use, with its four idle executor threads
SELECT version, threads FROM ch.engine_info();
