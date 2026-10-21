#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The cluster leg (ch_table_engines.md CH6, Appendix D), in a dev container
# of the port's image: a coordinator and CHTE_SEGMENTS primaries of the
# Cloudberry port in one container, each primary with a mirror streaming
# from it, as the port's own suites make them (its pg19/test/fts), the
# extension preloaded on every node before gp_core, and on it:
#
#   order    the extension listed after gp_core: a node of a cluster refuses
#            to start, saying why; a single node of the port starts, and its
#            tables work
#   ddl      CREATE EXTENSION on every node, its catalogs each node's own; a
#            session's ch.* settings on the segments; the engines' options
#            on every node, of CREATE TABLE in both stores and for the
#            engines with options of their own, CREATE TABLE AS, ALTER
#            TABLE ... SET, and a store a session's ch.default_storage gives
#   rows     rows the coordinator sends the segments -- INSERT ... SELECT,
#            VALUES, COPY, into a table with a BEFORE trigger, which a
#            segment writes row by row -- each statement two phases or one,
#            rolled back, with savepoints; a workload's answers over
#            mergetree in both stores as over heap, under the planner's
#            route and under ORCA; ChScan on the segments
#   dml      DELETE, UPDATE, an UPDATE of the distribution key, SELECT ...
#            FOR UPDATE and an index, answering as heap; TRUNCATE; DROP,
#            after which the files store's directories leave the segments
#            once VACUUM has pruned their catalog rows
#   merges   the merge workers on every segment: many inserts' parts merged
#            on each, the answers heap's; a merge on a segment, in a
#            transaction of the segment's own, beside a distributed snapshot
#            that does not see an insert yet, which reads the table without
#            it all the same
#   expand   ALTER TABLE ... SET DISTRIBUTED BY and SET WITH (REORGANIZE); a
#            table made on one segment spread over all by EXPAND TABLE; a
#            segment added to the running cluster, the tables expanded onto
#            it, answering as heap
#   mirrors  every write of the parts before replayed on the mirrors, both
#            stores; a primary stopped, its mirror promoted by FTS, the
#            tables read and written there as before, and the new primary's
#            merge workers started
#
#   CHTE_SEGMENTS       3
#   CHTE_CLUSTER_PARTS  the parts run, in that order: all of them
#   CHTE_CLUSTER_ROWS   rows of each table: 20000
#   KEEP                set: the cluster's directory kept for a look
#   CHTE_CLUSTER_HOLD   seconds the cluster stays up where a table's answers
#                       first differ from heap's, for a look from another
#                       shell in the container (docker exec)
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/lib.sh"
WORK="${CHTE_WORK:-/work}"
"$here/build.sh" > "$WORK/build.log" 2>&1 || { cat "$WORK/build.log"; exit 1; }
grep -E "^(building|the port)" "$WORK/build.log"

NSEG="${CHTE_SEGMENTS:-3}"
PARTS="${CHTE_CLUSTER_PARTS:-order ddl rows dml merges expand mirrors}"
ROWS="${CHTE_CLUSTER_ROWS:-20000}"
PRELOAD="ch_table_engines,gp_core,gp_orca,gp_sql"
ROOT=/tmp/chte-cluster
CONF="$ROOT/gp_cluster.conf"
SECRET="chte-cluster-$(od -An -tx8 -N8 /dev/urandom | tr -d ' \n')"
BASEPORT=6500
SLOT=internal_wal_replication_slot
DB=chte_cluster
pass=0
fail=0
rm -rf "$ROOT"
mkdir -p "$ROOT"

has_part() { case " $PARTS " in *" $1 "*) return 0 ;; esac; return 1; }
ok()    { printf '  ok     %s\n' "$1"; pass=$((pass + 1)); }
notok() { printf '  NOT OK %s\n' "$1"; [ -n "${2:-}" ] && printf '%s\n' "$2" | sed 's/^/         /' | head -20; fail=$((fail + 1)); }
check() {					# check <what> <got> <want>
	if [ "$2" = "$3" ]; then ok "$1"; else notok "$1" "got:  $2
want: $3"; fi
}

# node <n>: 0 the coordinator, 1..NSEG the primaries of contents 0..NSEG-1,
# NSEG+1..2*NSEG their mirrors, 2*NSEG+1 a segment added to the running
# cluster (expand), of content NSEG.
datadir() { echo "$ROOT/node$1/data"; }
sockdir() { echo "$ROOT/node$1/sock"; }
port()    { echo $((BASEPORT + $1)); }
dbid()    { echo $(($1 + 1)); }
content() {
	if [ "$1" -eq 0 ]; then echo -1
	elif [ "$1" -le "$NSEG" ]; then echo $(($1 - 1))
	elif [ "$1" -le $((2 * NSEG)) ]; then echo $(($1 - NSEG - 1))
	else echo $(($1 - NSEG - 1)); fi
}
logfile() { echo "$ROOT/node$1.log"; }
ADDED=$((2 * NSEG + 1))
declare -a PRIMARY			# content -> the node that is its primary
for c in $(seq 0 $((NSEG - 1))); do PRIMARY[$c]=$((c + 1)); done
primaries() { local c; for c in "${!PRIMARY[@]}"; do echo "${PRIMARY[$c]}"; done; }

cleanup() {
	exec 7>&- 2> /dev/null
	for n in $(seq 0 "$ADDED") single; do
		[ -d "$(datadir "$n")" ] && "$BINDIR/pg_ctl" -D "$(datadir "$n")" -m immediate stop > /dev/null 2>&1
	done
	mkdir -p "$WORK/cluster"
	cp "$ROOT"/node*.log "$WORK/cluster/" 2> /dev/null
	[ -n "${KEEP:-}" ] && echo "kept: $ROOT" || rm -rf "$ROOT"
}
trap cleanup EXIT

# q <node> <sql>: in the leg's database, -At, errors included; qdb names another
q()   { "$BINDIR/psql" -X -q -At -h "$(sockdir "$1")" -p "$(port "$1")" -U postgres -d "$DB" -c "$2" 2>&1; }
qdb() { "$BINDIR/psql" -X -q -At -h "$(sockdir "$1")" -p "$(port "$1")" -U postgres -d "$2" -c "$3" 2>&1; }
# qf <node>: a script on standard input, in one session
qf()  { "$BINDIR/psql" -X -q -At -h "$(sockdir "$1")" -p "$(port "$1")" -U postgres -d "$DB" -f - 2>&1; }
# each primary's answer, in its own session of the segment's: "<content>:<answer> ..."
qs() {
	local c
	for c in "${!PRIMARY[@]}"; do printf '%s:%s ' "$c" "$(q "${PRIMARY[$c]}" "$1" | tr '\n' ',')"; done
}
# whether every primary answers as the coordinator does
same_everywhere() {			# same_everywhere <what> <sql>
	local want got c bad=""
	want=$(q 0 "$2")
	for c in "${!PRIMARY[@]}"; do
		got=$(q "${PRIMARY[$c]}" "$2")
		[ "$got" = "$want" ] || bad="$bad
content $c: $got"
	done
	case "$want" in *ERROR*) bad="$bad
(an error)" ;; esac
	if [ -z "$bad" ] && [ -n "$want" ]; then ok "$1"; else notok "$1" "coordinator: $want$bad"; fi
}
wait_for() {				# wait_for <node> <sql> <want> [seconds]
	local n="$1" sql="$2" want="$3" t="${4:-60}" out
	for _ in $(seq $((t * 5))); do
		out=$(q "$n" "$sql")
		[ "$out" = "$want" ] && return 0
		sleep 0.2
	done
	echo "$out"
	return 1
}

node_settings() {			# node_settings <node> <shared_preload_libraries>
	local n="$1"
	echo "shared_preload_libraries = '$2'"
	echo "unix_socket_directories = '$(sockdir "$n")'"
	echo "listen_addresses = ''"
	echo "port = $(port "$n")"
	echo "gp.cluster_config = '$CONF'"
	echo "gp.dbid = $(dbid "$n")"
	echo "gp.cluster_secret = '$SECRET'"
	echo "max_prepared_transactions = 100"
	echo "max_connections = 200"
	echo "max_worker_processes = 32"
	echo "shared_buffers = 128MB"
	echo "fsync = off"
	# the merge workers on every node, a pass a second (the merges part)
	echo "ch.max_merge_workers = 2"
	echo "ch.merge_naptime = 1"
	[ "$n" -eq 0 ] && echo "gp.role = 'dispatch'"
	true
}
start_node() { "$BINDIR/pg_ctl" -D "$(datadir "$1")" -l "$(logfile "$1")" -w -t 120 start > /dev/null 2>&1 7>&-; }
stop_node()  { "$BINDIR/pg_ctl" -D "$(datadir "$1")" -m "${2:-fast}" -w -t 60 stop > /dev/null 2>&1; }

# A node made a mirror of another (the port's pg19/test/fts): its copy, taken
# with pg_basebackup, a hot standby streaming from it as gp_walreceiver from
# its slot -- the extension preloaded on it as on the primary, whose
# configuration it copies, for the files store's WAL.
make_mirror() {				# make_mirror <n> <of n> [create-slot]
	local m="$1" p="$2" slotopt=(-S "$SLOT")
	[ "${3:-}" = create-slot ] && slotopt=(-C -S "$SLOT")
	rm -rf "$(datadir "$m")"
	mkdir -p "$(sockdir "$m")"
	"$BINDIR/pg_basebackup" -D "$(datadir "$m")" -h "$(sockdir "$p")" -p "$(port "$p")" -U postgres \
		-X stream -c fast "${slotopt[@]}" > "$ROOT/basebackup$m.log" 2>&1 || return 1
	grep -v -E '^(port|unix_socket_directories|gp\.dbid|primary_conninfo|primary_slot_name|hot_standby) ' \
		"$(datadir "$m")/postgresql.auto.conf" > "$ROOT/auto.conf"
	{
		cat "$ROOT/auto.conf"
		echo "port = $(port "$m")"
		echo "unix_socket_directories = '$(sockdir "$m")'"
		echo "gp.dbid = $(dbid "$m")"
		echo "hot_standby = on"
		echo "primary_conninfo = 'host=$(sockdir "$p") port=$(port "$p") user=postgres application_name=gp_walreceiver'"
		echo "primary_slot_name = '$SLOT'"
	} > "$(datadir "$m")/postgresql.auto.conf"
	touch "$(datadir "$m")/standby.signal"
	start_node "$m"
}

# A session of the coordinator's that stays open, fed through a fifo: a
# transaction left open while other sessions write and merge.
session_open() {
	rm -f "$ROOT/s.in" "$ROOT/s.out"
	mkfifo "$ROOT/s.in"
	"$BINDIR/psql" -X -q -At -h "$(sockdir 0)" -p "$(port 0)" -U postgres -d "$DB" \
		-v ON_ERROR_STOP=0 -f "$ROOT/s.in" > "$ROOT/s.out" 2>&1 &
	SESSION_PID=$!
	exec 7> "$ROOT/s.in"
}
# session_ask <sql>: its answer, the lines the session wrote before a marker
session_ask() {
	local mark="mark-$RANDOM$RANDOM" from
	from=$(wc -l < "$ROOT/s.out")
	printf '%s\n\\echo %s\n' "$1" "$mark" >&7
	for _ in $(seq 600); do
		grep -q -x -e "$mark" "$ROOT/s.out" 2> /dev/null && break
		sleep 0.1
	done
	tail -n +$((from + 1)) "$ROOT/s.out" | grep -v -x -e "$mark"
}
session_close() { exec 7>&-; wait "$SESSION_PID" 2> /dev/null; }

echo "the cluster leg: a coordinator and $NSEG primaries, each with a mirror; $PRELOAD on every node"
{
	echo "# dbid content role host port datadir"
	for n in $(seq 0 $((2 * NSEG))); do
		role=p
		[ "$n" -gt "$NSEG" ] && role=m
		echo "$(dbid "$n") $(content "$n") $role $(sockdir "$n") $(port "$n") $(datadir "$n")"
	done
} > "$CONF"
for n in $(seq 0 "$NSEG"); do
	mkdir -p "$(sockdir "$n")"
	"$BINDIR/initdb" -D "$(datadir "$n")" -N -U postgres --locale=C --encoding=UTF8 > "$ROOT/initdb$n.log" 2>&1 \
		|| { echo "initdb failed for node $n"; tail -20 "$ROOT/initdb$n.log"; exit 1; }
done

###############################################################################
if has_part order; then
echo "order: the extension and gp_core in shared_preload_libraries"
###############################################################################
	# A node of the cluster with the extension after gp_core: it would send
	# the segments DDL without the engines' options, and refuses to start.
	node_settings 1 "gp_core,gp_orca,gp_sql,ch_table_engines" > "$(datadir 1)/postgresql.auto.conf"
	if start_node 1; then
		notok "a node of a cluster with ch_table_engines after gp_core refuses to start" "it started"
		stop_node 1 immediate
	else
		check "a node of a cluster with ch_table_engines after gp_core refuses to start, saying why" \
			"$(grep -c 'ch_table_engines must come before gp_core in shared_preload_libraries on a node of a cluster' "$(logfile 1)")" 1
	fi
	mv "$(logfile 1)" "$ROOT/node1-order.log"

	# A single node of the port -- gp_core, no cluster -- sends nothing, and
	# takes either order.
	mkdir -p "$ROOT/nodesingle/sock"
	"$BINDIR/initdb" -D "$(datadir single)" -N -U postgres --locale=C --encoding=UTF8 > "$ROOT/initdb-single.log" 2>&1
	{
		echo "shared_preload_libraries = 'gp_core,gp_orca,ch_table_engines'"
		echo "unix_socket_directories = '$(sockdir single)'"
		echo "listen_addresses = ''"
		echo "port = $((BASEPORT + 99))"
		echo "fsync = off"
	} >> "$(datadir single)/postgresql.auto.conf"
	if "$BINDIR/pg_ctl" -D "$(datadir single)" -l "$ROOT/nodesingle.log" -w -t 120 start > /dev/null 2>&1; then
		out=$("$BINDIR/psql" -X -q -At -h "$(sockdir single)" -p $((BASEPORT + 99)) -U postgres -d postgres 2>&1 <<-SQL
			SET client_min_messages = warning;
			CREATE EXTENSION gp_core;
			CREATE EXTENSION ch_table_engines;
			CREATE TABLE t (a int, b text) USING mergetree WITH (order_by = 'a');
			INSERT INTO t SELECT g, 'x' || g FROM generate_series(1, 1000) g;
			SELECT count(*), sum(a) FROM t;
			SELECT array_to_string(reloptions, ',') FROM pg_class WHERE relname = 't';
		SQL
		)
		check "a single node of the port takes ch_table_engines after gp_core, and its tables" "$(echo $out)" "1000|500500 order_by=a,storage=pages"
		"$BINDIR/pg_ctl" -D "$(datadir single)" -m fast -w stop > /dev/null 2>&1
	else
		notok "a single node of the port with ch_table_engines after gp_core starts" "$(tail -5 "$ROOT/nodesingle.log")"
	fi
fi

###############################################################################
echo "the cluster starts: the primaries, a mirror of each, the coordinator"
###############################################################################
for n in $(seq 0 "$NSEG"); do
	node_settings "$n" "$PRELOAD" > "$(datadir "$n")/postgresql.auto.conf"
done
for n in $(seq 1 "$NSEG"); do
	start_node "$n" || { notok "node $n starts" "$(tail -5 "$(logfile "$n")")"; exit 1; }
done
for n in $(seq 1 "$NSEG"); do
	make_mirror $((n + NSEG)) "$n" create-slot ||
		{ notok "the mirror of node $n" "$(tail -5 "$ROOT/basebackup$((n + NSEG)).log") $(tail -5 "$(logfile $((n + NSEG)))")"; exit 1; }
done
start_node 0 || { notok "the coordinator starts" "$(tail -5 "$(logfile 0)")"; exit 1; }
for db in template1 postgres; do
	out=$(qdb 0 "$db" "SET client_min_messages = warning; CREATE EXTENSION gp_core")
	[ -n "$out" ] && { notok "CREATE EXTENSION gp_core in $db" "$out"; exit 1; }
done
streaming=1
for n in $(seq 1 "$NSEG"); do
	for _ in $(seq 300); do
		out=$(qdb "$n" postgres "SELECT count(*) FROM pg_stat_replication WHERE application_name = 'gp_walreceiver' AND state = 'streaming'")
		[ "$out" = 1 ] && break
		sleep 0.2
	done
	[ "$out" = 1 ] || streaming=0
done
check "each primary's mirror streams from it" "$streaming" 1
out=$(qdb 0 postgres "SELECT gp_request_fts_probe_scan()")
check "FTS brings every pair in sync" \
	"$(qdb 0 postgres "SELECT string_agg(content || ':' || role::text || mode::text || status::text, ' ' ORDER BY content, role DESC) FROM gp_segment_configuration WHERE content >= 0")" \
	"$(for c in $(seq 0 $((NSEG - 1))); do printf '%s:psu %s:msu ' "$c" "$c"; done | sed 's/ $//')"
out=$(qdb 0 postgres "CREATE DATABASE $DB")
[ -n "$out" ] && { notok "CREATE DATABASE" "$out"; exit 1; }
out=$(q 0 "SET client_min_messages = warning; CREATE EXTENSION gp_sql; CREATE EXTENSION ch_table_engines")
[ -n "$out" ] && { notok "CREATE EXTENSION ch_table_engines" "$out"; exit 1; }

# the leg's tables: their rows, heap's, and the same in each store
cat > "$ROOT/src.sql" <<SQL
SET client_min_messages = warning;
CREATE TABLE src AS
SELECT g AS id, g % 97 AS k, (g * 7919) % 100000 AS v, ((g % 1000) / 7.0)::numeric(12,4) AS n,
       'name ' || (g % 503) AS s, DATE '2020-01-01' + g % 1000 AS d, g % 3 = 0 AS b
FROM generate_series(1, $ROWS) g DISTRIBUTED BY (id);
COPY src TO '$ROOT/src.csv' CSV;
COPY (SELECT * FROM src WHERE id > $((ROWS / 2))) TO '$ROOT/src2.csv' CSV;
SQL
out=$(qf 0 < "$ROOT/src.sql")
[ -n "$out" ] && { notok "the rows" "$out"; exit 1; }

# the answers of a query over a table and over heap's, compared
QUERIES=(
	"SELECT count(*), sum(v), min(s), max(d), sum(n) FROM %t"
	"SELECT k, count(*), sum(v), avg(n)::numeric(20,4) FROM %t WHERE b GROUP BY k ORDER BY k"
	"SELECT s, count(DISTINCT k) FROM %t GROUP BY s HAVING count(*) > 5 ORDER BY s"
	"SELECT a.k, count(*) FROM %t a JOIN %t b ON a.id = b.v WHERE b.k < 10 GROUP BY a.k ORDER BY a.k"
	"SELECT id, v FROM %t WHERE k = 7 AND id < 5000 ORDER BY id"
	"SELECT id, s FROM %t ORDER BY v DESC, id LIMIT 20"
	"SELECT count(*), sum(a.v) FROM %t a WHERE EXISTS (SELECT 1 FROM %t b WHERE b.v = a.id AND b.k > a.k)"
	"SELECT d, sum(v), count(*) FROM %t WHERE d BETWEEN DATE '2020-03-01' AND DATE '2020-04-01' GROUP BY d ORDER BY d"
)
answers() {					# answers <table> <gp.optimizer>: each query's answer's md5
	local t="$1" opt="$2" sql
	for sql in "${QUERIES[@]}"; do
		q 0 "SET client_min_messages = warning; SET gp.optimizer = $opt; ${sql//%t/$t}" | md5sum | cut -c1-12
	done | tr '\n' ' '
}
same_as_heap() {			# same_as_heap <what> <table> <heap's table> [optimizers]
	local what="$1" t="$2" h="$3" opt want got sql
	for opt in ${4:-off on}; do
		want=$(answers "$h" "$opt")
		got=$(answers "$t" "$opt")
		check "$what, as heap's, gp.optimizer = $opt" "$got" "$want"
		[ "$got" = "$want" ] && continue
		# the first query whose answers differ, and each segment's rows
		for sql in "${QUERIES[@]}"; do
			[ "$(q 0 "SET client_min_messages = warning; SET gp.optimizer = $opt; ${sql//%t/$t}")" = "$(q 0 "SET client_min_messages = warning; SET gp.optimizer = $opt; ${sql//%t/$h}")" ] && continue
			echo "         $sql:"
			diff <(q 0 "SET gp.optimizer = $opt; ${sql//%t/$h}") <(q 0 "SET gp.optimizer = $opt; ${sql//%t/$t}") | head -6 | sed 's/^/           /'
			break
		done
		echo "         rows of each segment, $h / $t: $(q 0 "SELECT string_agg(c::text, ' ') FROM (SELECT gp_segment_id, count(*) AS c FROM $h GROUP BY 1 ORDER BY 1) s") / $(q 0 "SELECT string_agg(c::text, ' ') FROM (SELECT gp_segment_id, count(*) AS c FROM $t GROUP BY 1 ORDER BY 1) s")"
		[ -n "${CHTE_CLUSTER_HOLD:-}" ] && { echo "         held: $ROOT"; sleep "$CHTE_CLUSTER_HOLD"; }
	done
}
storage_of() {				# storage_of <node> <table>: its files store directory, relative to the data directory
	q "$1" "SELECT 'base/' || (SELECT oid FROM pg_database WHERE datname = current_database()) || '/ch_' || pg_relation_filenode('$2')"
}
dir_exists() {				# dir_exists <node> <path relative to its data directory>
	q "$1" "SELECT (pg_stat_file('$2', true)).isdir IS TRUE"
}

###############################################################################
if has_part ddl; then
echo "ddl: the extension, its settings and the engines' options on every node"
###############################################################################
	CATS="'ch.tables'::regclass, 'ch.parts'::regclass, 'ch.tombstones'::regclass, 'ch.tables_unlogged'::regclass, 'ch.parts_unlogged'::regclass, 'ch.tombstones_unlogged'::regclass"
	check "the extension on every node, of one version" \
		"$(qs "SELECT extversion FROM pg_extension WHERE extname = 'ch_table_engines'")" \
		"$(for c in $(seq 0 $((NSEG - 1))); do printf '%s:0.1, ' "$c"; done)"
	same_everywhere "the catalogs are each node's own: no distribution policy, no label of gp's, on any node" \
		"SELECT count(*) FROM pg_seclabels WHERE provider = 'gp' AND objoid IN ($CATS)"
	check "and the coordinator's policies have none of them" \
		"$(q 0 "SELECT count(*) FROM gp_distribution_policy WHERE localoid IN ($CATS)")" 0
	check "a session's ch.* settings reach the segments" \
		"$(q 0 "SET ch.default_storage = files; SET ch.enable_chscan = off; SET ch.fetch_cost = 7;
				SELECT string_agg(result, ',' ORDER BY content) FROM gp.exec_on_segments('SELECT current_setting(''ch.default_storage'') || ''/'' || current_setting(''ch.enable_chscan'') || ''/'' || current_setting(''ch.fetch_cost'')')")" \
		"$(for c in $(seq 0 $((NSEG - 1))); do printf 'files/off/7,'; done | sed 's/,$//')"

	out=$(qf 0 <<-SQL
		SET client_min_messages = warning;
		CREATE TABLE t_heap (LIKE src) DISTRIBUTED BY (id);
		CREATE TABLE t_pages (LIKE src) USING mergetree WITH (storage = 'pages', order_by = 'k, id', index_granularity = 1024) DISTRIBUTED BY (id);
		CREATE TABLE t_files (LIKE src) USING mergetree WITH (storage = 'files', order_by = 'k, id', compression = 'zstd') DISTRIBUTED BY (id);
		CREATE TABLE t_rep (id int, v int, ver int NOT NULL) USING replacingmergetree WITH (order_by = 'id', ver = 'ver') DISTRIBUTED BY (id);
		CREATE TABLE t_col (id int, v int, sign int8 NOT NULL) USING collapsingmergetree WITH (order_by = 'id', sign = 'sign') DISTRIBUTED BY (id);
		CREATE TABLE t_ctas USING mergetree WITH (order_by = 'd, id', storage = 'files') AS SELECT * FROM src DISTRIBUTED BY (id);
		CREATE TABLE t_alter (LIKE src) USING mergetree WITH (order_by = 'id') DISTRIBUTED BY (id);
		ALTER TABLE t_alter SET (compression = 'lz4hc(9)', index_granularity = 512);
		SET ch.default_storage = files;
		CREATE TABLE t_default (a int, b text) USING mergetree DISTRIBUTED BY (a);
	SQL
	)
	check "CREATE TABLE in both stores, of the engines with options of their own, CREATE TABLE AS, ALTER TABLE ... SET" "$out" ""
	for t in t_pages t_files t_rep t_col t_ctas t_alter t_default; do
		same_everywhere "$t's options and ClickHouse's definition alike on every node" \
			"SELECT array_to_string(reloptions, ',') || ' ' || md5(ch.definition('$t')) FROM pg_class WHERE oid = '$t'::regclass"
	done
	check "the options are the statements'" \
		"$(q 0 "SELECT string_agg(relname || ':' || array_to_string(reloptions, ','), ' ' ORDER BY relname) FROM pg_class WHERE relname IN ('t_pages', 't_files', 't_alter', 't_default')")" \
		"t_alter:order_by=id,storage=pages,compression=lz4hc(9),index_granularity=512 t_default:storage=files t_files:storage=files,order_by=k, id,compression=zstd t_pages:storage=pages,order_by=k, id,index_granularity=1024"
	check "CREATE TABLE AS wrote its rows on the segments" "$(q 0 "SELECT count(*) FROM t_ctas")" "$ROWS"
	out=$(q 0 "ALTER TABLE t_ctas SET (order_by = 'id')" | grep -o 'ERROR:.*')
	check "a defining option of a table a segment has rows of is refused" \
		"$out" "ERROR:  cannot change the storage, keys or engine options of \"t_ctas\" while it has data"
fi

###############################################################################
if has_part rows; then
echo "rows: writes the coordinator sends the segments, and their answers"
###############################################################################
	out=$(qf 0 <<-SQL
		INSERT INTO t_heap SELECT * FROM src;
		INSERT INTO t_pages SELECT * FROM src WHERE id <= $((ROWS / 2));
		COPY t_pages FROM '$ROOT/src2.csv' CSV;
		INSERT INTO t_files SELECT * FROM src;
	SQL
	)
	check "INSERT ... SELECT and COPY into heap and both stores" "$out" ""
	check "every segment has rows of each" \
		"$(q 0 "SELECT count(DISTINCT gp_segment_id) FROM t_pages") $(q 0 "SELECT count(DISTINCT gp_segment_id) FROM t_files")" "$NSEG $NSEG"
	same_as_heap "the pages store" t_pages t_heap
	same_as_heap "the files store" t_files t_heap
	same_as_heap "CREATE TABLE AS's" t_ctas src off
	out=$(q 0 "SET ch.enable_chscan = off; SELECT count(*), sum(v) FROM t_pages")
	check "without ChScan, the segments' plain scans" "$out" "$(q 0 "SELECT count(*), sum(v) FROM t_heap")"
	check "a segment plans ChScan for its share of a query on its own" \
		"$(q "${PRIMARY[0]}" "EXPLAIN (COSTS OFF) SELECT count(*) FROM t_pages WHERE k = 5" | grep -c 'Custom Scan (ChScan)')" 1

	# a table with a BEFORE trigger: a segment writes the rows it is sent row
	# by row, and its transaction has an ID as the statement ends
	out=$(qf 0 <<-SQL
		SET client_min_messages = warning;
		CREATE TABLE t_trig (LIKE src) USING mergetree WITH (order_by = 'id') DISTRIBUTED BY (id);
		CREATE FUNCTION bump() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN NEW.v := NEW.v + 1; RETURN NEW; END \$\$;
		CREATE TRIGGER t_trig_bump BEFORE INSERT ON t_trig FOR EACH ROW EXECUTE FUNCTION bump();
		COPY t_trig FROM '$ROOT/src.csv' CSV;
		BEGIN;
		INSERT INTO t_trig SELECT * FROM src WHERE id <= 100;
		COMMIT;
		SELECT count(*), sum(v) FROM t_trig;
	SQL
	)
	check "a COPY and an INSERT into a table with a BEFORE trigger, committed in two phases" \
		"$out" "$(q 0 "SELECT count(*) + 100, sum(v + 1) + (SELECT sum(v + 1) FROM src WHERE id <= 100) FROM src")"

	out=$(qf 0 <<-SQL
		INSERT INTO t_pages VALUES ($((ROWS + 1)), 1, 1, 1, 'one', DATE '2021-01-01', true);
		BEGIN;
		INSERT INTO t_files SELECT * FROM src WHERE id <= 1000;
		INSERT INTO t_pages SELECT * FROM src WHERE id <= 1000;
		ROLLBACK;
		BEGIN;
		INSERT INTO t_files SELECT id + $ROWS, k, v, n, s, d, b FROM src WHERE id <= 300;
		SAVEPOINT s1;
		INSERT INTO t_files SELECT * FROM src WHERE id <= 1000;
		ROLLBACK TO SAVEPOINT s1;
		INSERT INTO t_files SELECT id + 2 * $ROWS, k, v, n, s, d, b FROM src WHERE id <= 200;
		COMMIT;
		SELECT count(*) FROM t_pages;
		SELECT count(*) FROM t_files;
	SQL
	)
	check "a row to one segment, a transaction rolled back, a savepoint rolled back to" \
		"$(echo $out)" "$((ROWS + 1)) $((ROWS + 500))"
	out=$(qf 0 <<-SQL
		DELETE FROM t_pages WHERE id = $((ROWS + 1));
		DELETE FROM t_files WHERE id > $ROWS;
	SQL
	)
	same_as_heap "both stores after them" t_files t_heap on
fi

###############################################################################
if has_part dml; then
echo "dml: deletions, updates, locks and indexes on the segments"
###############################################################################
	DML=(
		"DELETE FROM %t WHERE k = 3"
		"UPDATE %t SET v = v + 1, s = s || '+' WHERE id % 10 = 0"
		"UPDATE %t SET id = id + 1000000 WHERE id % 97 = 1"
		"DELETE FROM %t WHERE id IN (SELECT id FROM src WHERE v < 1000)"
	)
	for sql in "${DML[@]}"; do
		for t in t_heap t_pages t_files; do
			out=$(q 0 "${sql//%t/$t}")
			case "$out" in ERROR*) notok "$t: $sql" "$out" ;; esac
		done
	done
	same_as_heap "deletions, updates and an update of the distribution key: the pages store" t_pages t_heap
	same_as_heap "and the files store" t_files t_heap
	check "SELECT ... FOR UPDATE" \
		"$(q 0 "BEGIN; SELECT count(*) FROM (SELECT id FROM t_pages WHERE id < 100 FOR UPDATE) l; COMMIT;")" \
		"$(q 0 "SELECT count(*) FROM t_heap WHERE id < 100")"
	out=$(q 0 "CREATE INDEX t_pages_v ON t_pages (v)")
	check "an index, made on every segment" "$out" ""
	check "a segment scans it" \
		"$(q "${PRIMARY[0]}" "SET enable_seqscan = off; SET ch.enable_chscan = off; EXPLAIN (COSTS OFF) SELECT * FROM t_pages WHERE v = 4242" | grep -c -E 'Index (Only )?Scan (using|on) t_pages_v')" 1
	check "and answers through it as heap" \
		"$(q 0 "SET enable_seqscan = off; SET ch.enable_chscan = off; SELECT count(*), sum(id) FROM t_pages WHERE v BETWEEN 4000 AND 4400")" \
		"$(q 0 "SELECT count(*), sum(id) FROM t_heap WHERE v BETWEEN 4000 AND 4400")"

	# TRUNCATE and DROP of the files store: each segment's prepared part of
	# the statement leaves its old directory, which goes once VACUUM has
	# pruned the storage's catalog row (ch.remove_orphans())
	for c in "${!PRIMARY[@]}"; do old[$c]=$(storage_of "${PRIMARY[$c]}" t_files); done
	out=$(qf 0 <<-SQL
		CREATE TABLE t_files2 (LIKE src) USING mergetree WITH (storage = 'files', order_by = 'id') DISTRIBUTED BY (id);
		INSERT INTO t_files2 SELECT * FROM t_files;
		TRUNCATE t_files;
		SELECT count(*) FROM t_files;
		INSERT INTO t_files SELECT * FROM t_files2;
	SQL
	)
	check "TRUNCATE, and the table written again" "$out" "0"
	same_as_heap "the files store after TRUNCATE" t_files t_heap on
	for c in "${!PRIMARY[@]}"; do drop[$c]=$(storage_of "${PRIMARY[$c]}" t_files2); done
	check "DROP TABLE" "$(q 0 "DROP TABLE t_files2")" ""
	# a statement sent after them: the segments then know every snapshot sees
	# them committed (gp_core's gp_dtx.c), and VACUUM may prune their rows
	for c in "${!PRIMARY[@]}"; do before[$c]="$(dir_exists "${PRIMARY[$c]}" "${old[$c]}")$(dir_exists "${PRIMARY[$c]}" "${drop[$c]}")"; done
	q 0 "SELECT count(*) FROM t_heap" > /dev/null
	q 0 "VACUUM ch.tables" > /dev/null
	gone=""
	for c in "${!PRIMARY[@]}"; do
		n="${PRIMARY[$c]}"
		q "$n" "VACUUM ch.tables" > /dev/null
		q "$n" "SELECT ch.remove_orphans()" > /dev/null
		gone="$gone $c:${before[$c]}/$(dir_exists "$n" "${old[$c]}")$(dir_exists "$n" "${drop[$c]}")"
	done
	check "the old storages' directories, left by PREPARE on each segment, go once VACUUM pruned their rows" \
		"$gone" "$(for c in "${!PRIMARY[@]}"; do printf ' %s:tt/ff' "$c"; done)"
	check "the table that took its place keeps its own" \
		"$(qs "SELECT (pg_stat_file('base/' || (SELECT oid FROM pg_database WHERE datname = current_database()) || '/ch_' || pg_relation_filenode('t_files'), true)).isdir")" \
		"$(for c in "${!PRIMARY[@]}"; do printf '%s:t, ' "$c"; done)"
fi

###############################################################################
if has_part merges; then
echo "merges: the merge workers of the segments, and distributed snapshots"
###############################################################################
	out=$(qf 0 <<-SQL
		SET client_min_messages = warning;
		CREATE TABLE t_mheap (LIKE src) DISTRIBUTED BY (id);
		CREATE TABLE t_m (LIKE src) USING mergetree WITH (order_by = 'id') DISTRIBUTED BY (id);
		CREATE TABLE t_mf (LIKE src) USING mergetree WITH (order_by = 'id', storage = 'files') DISTRIBUTED BY (id);
	SQL
	)
	for i in $(seq 0 29); do
		lo=$((i * ROWS / 30 + 1)); hi=$(((i + 1) * ROWS / 30))
		for t in t_mheap t_m t_mf; do
			q 0 "INSERT INTO $t SELECT * FROM src WHERE id BETWEEN $lo AND $hi" > /dev/null
		done
	done
	check "30 inserts: each segment's share of their rows in its parts" \
		"$(qs "SELECT sum(rows) FROM ch.parts('t_m')" | tr ',' '\n' | awk -F: 'NF == 2 { s += $2 } END { print s }')" "$ROWS"
	# The newest part waits for a statement sent after it, which tells the
	# segments that no snapshot is without it (gp_core's gp_dtx.c): a
	# cluster's coordinator sends them all the time, and here one a second.
	want=$(for c in "${!PRIMARY[@]}"; do printf '%s:t,t, ' "$c"; done)
	for _ in $(seq 120); do
		got=$(qs "SELECT count(*) < 4 FROM ch.parts('t_m') UNION ALL SELECT count(*) < 4 FROM ch.parts('t_mf')")
		[ "$got" = "$want" ] && break
		q 0 "SELECT count(*) FROM t_mheap" > /dev/null
		sleep 1
	done
	check "each segment's merge workers merge them, in both stores" "$got" "$want"
	[ "$got" = "$want" ] || notok "the parts then" "$(qs "SELECT string_agg(name || '/' || xmin, ' ') FROM ch.parts WHERE relfilenumber = pg_relation_filenode('t_mf')")"
	check "its merge workers are the segment's own" \
		"$(qs "SELECT count(*) > 0 FROM pg_stat_activity WHERE backend_type = 'ch_table_engines merge worker' AND datname = current_database()")" \
		"$(for c in "${!PRIMARY[@]}"; do printf '%s:t, ' "$c"; done)"
	same_as_heap "the merged parts" t_m t_mheap
	same_as_heap "the merged parts of the files store" t_mf t_mheap off
	check "ch.optimize(final) on every segment, through gp.exec_on_segments" \
		"$(q 0 "SELECT count(*) FROM gp.exec_on_segments('SELECT ch.optimize(''t_m'', true)')") $(qs "SELECT count(*) FROM ch.parts('t_m')")" \
		"$NSEG $(for c in "${!PRIMARY[@]}"; do printf '%s:1, ' "$c"; done)"

	# a rewrite's storage is the table's after it, though its row names the
	# transient relation the rewrite wrote it for: the workers merge on
	q 0 "VACUUM FULL t_m" > /dev/null
	for i in $(seq 0 19); do
		q 0 "INSERT INTO t_m SELECT id + 1000000, k, v, n, s, d, b FROM src WHERE id % 20 = $i" > /dev/null
		q 0 "INSERT INTO t_mheap SELECT id + 1000000, k, v, n, s, d, b FROM src WHERE id % 20 = $i" > /dev/null
	done
	want=$(for c in "${!PRIMARY[@]}"; do printf '%s:t, ' "$c"; done)
	for _ in $(seq 120); do
		got=$(qs "SELECT count(*) < 4 FROM ch.parts('t_m')")
		[ "$got" = "$want" ] && break
		q 0 "SELECT count(*) FROM t_mheap" > /dev/null
		sleep 1
	done
	check "after VACUUM FULL, the workers merge the table's new parts" "$got" "$want"
	same_as_heap "the table rewritten and merged" t_m t_mheap on

	# a database dropped while each segment's merge worker is connected to it
	out=$(qdb 0 postgres "CREATE DATABASE chte_drop")
	out="$out$(qdb 0 chte_drop "SET client_min_messages = warning; CREATE EXTENSION gp_sql; CREATE EXTENSION ch_table_engines;
			CREATE TABLE d (a int) USING mergetree DISTRIBUTED BY (a); INSERT INTO d SELECT generate_series(1, 1000)")"
	want=$(for c in "${!PRIMARY[@]}"; do printf '%s:t, ' "$c"; done)
	for _ in $(seq 150); do
		got=$(for c in "${!PRIMARY[@]}"; do printf '%s:%s, ' "$c" "$(qdb "${PRIMARY[$c]}" postgres "SELECT count(*) > 0 FROM pg_stat_activity WHERE datname = 'chte_drop' AND backend_type = 'ch_table_engines merge worker'")"; done)
		[ "$got" = "$want" ] && break
		sleep 0.4
	done
	check "each segment's merge worker connected to a new database" "$got" "$want"
	check "DROP DATABASE while they are: they leave it" "$out$(qdb 0 postgres "DROP DATABASE chte_drop")" ""

	# A distributed snapshot taken before an insert commits does not see the
	# insert; a merge on a segment in a transaction of the segment's own --
	# a merge worker's, or ch.optimize() in a session of the segment's --
	# which that snapshot sees committed, must not give it the insert's rows.
	out=$(qf 0 <<-SQL
		SET client_min_messages = warning;
		CREATE TABLE t_ds (LIKE src) USING mergetree WITH (order_by = 'id') DISTRIBUTED BY (id);
		INSERT INTO t_ds SELECT * FROM src WHERE id <= 1000;
		INSERT INTO t_ds SELECT * FROM src WHERE id BETWEEN 1001 AND 2000;
		SELECT count(*) FROM t_ds;
	SQL
	)
	session_open
	opened=$(session_ask "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1;")
	q 0 "INSERT INTO t_ds SELECT * FROM src WHERE id BETWEEN 2001 AND 3000" > /dev/null
	merged=""
	for n in $(primaries); do merged="$merged $(q "$n" "SELECT ch.optimize('t_ds', true)")"; done
	during=$(session_ask "SELECT count(*) FROM t_ds;")
	session_ask "COMMIT;" > /dev/null
	after=$(session_ask "SELECT count(*) FROM t_ds;")
	session_close
	check "a snapshot from before an insert reads the table without it, though each segment merged its parts in a transaction of its own" \
		"$during/$after" "2000/3000"
	merged=""
	for n in $(primaries); do merged="$merged$(q "$n" "SELECT ch.optimize('t_ds', true) > 0; SELECT count(*) FROM ch.parts('t_ds')" | tr '\n' ' ')"; done
	check "and once no snapshot is without it, its parts merge with the others'" \
		"$merged" "$(for n in $(primaries); do printf 't 1 '; done)"
fi

###############################################################################
if has_part expand; then
echo "expand: the rows redistributed, a table spread, a segment added"
###############################################################################
	out=$(qf 0 <<-SQL
		ALTER TABLE t_pages SET DISTRIBUTED BY (k);
		ALTER TABLE t_heap SET DISTRIBUTED BY (k);
		ALTER TABLE t_files SET WITH (REORGANIZE = true);
	SQL
	)
	check "SET DISTRIBUTED BY and SET WITH (REORGANIZE)" "$out" ""
	same_as_heap "the pages store, distributed by another column" t_pages t_heap on
	same_as_heap "the files store, reorganized" t_files t_heap on
	same_everywhere "its options alike on every node after" \
		"SELECT array_to_string(reloptions, ',') || ' ' || md5(ch.definition('t_pages')) FROM pg_class WHERE oid = 't_pages'::regclass"

	out=$(qf 0 <<-SQL
		SET client_min_messages = warning;
		CREATE EXTENSION gp_debug_numsegments;
		SELECT gp_debug_set_create_table_default_numsegments(1) IS NOT NULL;
		CREATE TABLE t_one (LIKE src) USING mergetree WITH (order_by = 'id', storage = 'files') DISTRIBUTED BY (id);
		CREATE TABLE t_one_heap (LIKE src) DISTRIBUTED BY (id);
		SELECT gp_debug_reset_create_table_default_numsegments();
		INSERT INTO t_one SELECT * FROM src;
		INSERT INTO t_one_heap SELECT * FROM src;
	SQL
	)
	check "a table made on one segment" "$(q 0 "SELECT count(DISTINCT gp_segment_id), count(*) FROM t_one")" "1|$ROWS"
	out=$(qf 0 <<-SQL
		ALTER TABLE t_one EXPAND TABLE;
		ALTER TABLE t_one_heap EXPAND TABLE;
	SQL
	)
	check "spread over every segment by EXPAND TABLE" "$out/$(q 0 "SELECT count(DISTINCT gp_segment_id), count(*) FROM t_one")" "/$NSEG|$ROWS"
	same_as_heap "its rows" t_one t_one_heap on

	# a segment added to the running cluster, as gpexpand adds one: a copy of
	# the coordinator's catalogs and none of its rows, then EXPAND TABLE
	n=$ADDED
	out=$(q 0 "SELECT gp.expand_pin_numsegments() > 0")
	mkdir -p "$(sockdir "$n")"
	if "$BINDIR/pg_basebackup" -D "$(datadir "$n")" -h "$(sockdir 0)" -p "$(port 0)" -U postgres \
		   -X stream -c fast > "$ROOT/basebackup$n.log" 2>&1; then
		rm -f "$(datadir "$n")"/gpsegconfig_dump*
		node_settings "$n" "$PRELOAD" > "$(datadir "$n")/postgresql.auto.conf"
		echo "$(dbid "$n") $(content "$n") p $(sockdir "$n") $(port "$n") $(datadir "$n")" >> "$CONF"
		start_node "$n" || notok "the added segment starts" "$(tail -5 "$(logfile "$n")")"
		out=$(qdb 0 postgres "BEGIN; SELECT gp_add_segment($(dbid "$n")::int2, $(content "$n")::int2, 'p', 'p', 'n', 'u', $(port "$n"), '$(sockdir "$n")', '$(sockdir "$n")', '$(datadir "$n")'); COMMIT;" | grep -c ERROR)
		check "a segment added to the running cluster" "$out" 0
		PRIMARY[$NSEG]=$n
		out=$(qf 0 <<-SQL
			ALTER TABLE t_pages EXPAND TABLE;
			ALTER TABLE t_heap EXPAND TABLE;
			ALTER TABLE t_files EXPAND TABLE;
			ALTER TABLE t_m EXPAND TABLE;
			ALTER TABLE t_mheap EXPAND TABLE;
			SELECT count(DISTINCT gp_segment_id) FROM t_pages;
			SELECT count(DISTINCT gp_segment_id) FROM t_files;
		SQL
		)
		check "the tables of both stores expanded onto it" "$(echo $out)" "$((NSEG + 1)) $((NSEG + 1))"
		same_as_heap "the pages store over every segment" t_pages t_heap
		same_as_heap "the files store over every segment" t_files t_heap on
		same_as_heap "the merged table" t_m t_mheap on
		check "the new segment's share of each in its own parts" \
			"$(q "$n" "SELECT count(*) > 0 FROM ch.parts('t_pages')") $(q "$n" "SELECT count(*) > 0 FROM ch.parts('t_files')")" "t t"
		check "a new table written there too" \
			"$(q 0 "CREATE TABLE t_after (LIKE src) USING mergetree WITH (order_by = 'id') DISTRIBUTED BY (id); INSERT INTO t_after SELECT * FROM src; SELECT count(DISTINCT gp_segment_id), count(*) FROM t_after")" \
			"$((NSEG + 1))|$ROWS"
	else
		notok "the coordinator copied for the added segment" "$(tail -5 "$ROOT/basebackup$n.log")"
	fi
fi

###############################################################################
if has_part mirrors; then
echo "mirrors: both stores replayed, a primary failed over to its mirror"
###############################################################################
	caught=1
	for c in $(seq 0 $((NSEG - 1))); do
		p="${PRIMARY[$c]}"
		lsn=$(qdb "$p" postgres "SELECT pg_current_wal_lsn()")
		wait_for $((c + 1 + NSEG)) "SELECT pg_last_wal_replay_lsn() >= '$lsn'" t 60 > /dev/null || caught=0
	done
	check "every mirror has replayed its primary's WAL" "$caught" 1
	m=$((NSEG + 1))
	check "a mirror, a hot standby, reads both stores' parts as its primary wrote them" \
		"$(q "$m" "SELECT count(*), sum(v) FROM t_files") $(q "$m" "SELECT count(*), sum(v) FROM t_pages")" \
		"$(q 1 "SELECT count(*), sum(v) FROM t_files") $(q 1 "SELECT count(*), sum(v) FROM t_pages")"
	stop_node 1 immediate
	qdb 0 postgres "SELECT gp_request_fts_probe_scan()" > /dev/null
	check "the primary of content 0 stopped, FTS promotes its mirror" \
		"$(qdb 0 postgres "SELECT role::text || preferred_role::text || mode::text || status::text FROM gp_segment_configuration WHERE dbid = $(dbid "$m")")" "pmnu"
	wait_for "$m" "SELECT pg_is_in_recovery()" f 60 > /dev/null
	PRIMARY[0]=$m
	same_as_heap "the pages store, content 0's rows from the promoted mirror" t_pages t_heap on
	same_as_heap "the files store" t_files t_heap on
	out=$(qf 0 <<-SQL
		INSERT INTO t_pages SELECT id + 3000000, k, v, n, s, d, b FROM src WHERE id <= 2000;
		INSERT INTO t_heap SELECT id + 3000000, k, v, n, s, d, b FROM src WHERE id <= 2000;
		INSERT INTO t_files SELECT id + 3000000, k, v, n, s, d, b FROM src WHERE id <= 2000;
		DELETE FROM t_pages WHERE k = 11;
		DELETE FROM t_heap WHERE k = 11;
		DELETE FROM t_files WHERE k = 11;
	SQL
	)
	check "written after the failover" "$out" ""
	same_as_heap "the pages store, written after it" t_pages t_heap on
	same_as_heap "the files store, written after it" t_files t_heap on
	check "the promoted mirror runs merge workers of its own" \
		"$(wait_for "$m" "SELECT count(*) > 0 FROM pg_stat_activity WHERE backend_type = 'ch_table_engines merge launcher'" t 60 > /dev/null && echo t)" t
fi

###############################################################################
echo "the nodes' logs"
###############################################################################
bad=""
for f in "$ROOT"/node[0-9]*.log; do
	out=$(grep -E "TRAP:|PANIC|terminated by signal|server process .* was terminated" "$f" | grep -v 'terminated by signal 9: Killed' | head -3)
	[ -n "$out" ] && bad="$bad
$(basename "$f"): $out"
done
[ -z "$bad" ] && ok "no assertion, crash or PANIC in any node's log" || notok "the nodes' logs" "$bad"

echo
echo "cluster: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
