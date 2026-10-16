-- Without the library preloaded: temporary and UNLOGGED tables work; a
-- permanent one needs the resource manager a preloaded library registers.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS ch_table_engines;
CREATE TEMP TABLE np (a int) USING mergetree;
INSERT INTO np SELECT generate_series(1, 10);
SELECT count(*), sum(a) FROM np;
CREATE UNLOGGED TABLE npu (a int) USING mergetree;
INSERT INTO npu VALUES (1);
SELECT count(*) FROM npu;
CREATE TABLE npp (a int) USING mergetree;
DROP TABLE np, npu;
