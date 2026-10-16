# SPDX-License-Identifier: Apache-2.0
#
# What test/'s scripts share, sourced inside a dev container: a server of the
# container's PostgreSQL, made and started in a directory of its own,
# listening on a socket in it only (as pg_accel's test/vexec/lib.sh).
#
#   server_init <dir> [setting...]   initdb, and the settings appended
#   server_start <dir>               start it; 0 if it came up
#   server_stop <dir> [mode]
#   q <dir> <db> <sql>               psql -At, errors included

BINDIR="${PG_BINDIR:-$(dirname "$(command -v pg_config || echo /usr/local/pgsql/bin/pg_config)")}"
export LD_LIBRARY_PATH="$("$BINDIR/pg_config" --libdir)${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

server_init() {
	local dir="$1"; shift
	mkdir -p "$dir/sock"
	"$BINDIR/initdb" -D "$dir/data" -N -U postgres --locale=C --encoding=UTF8 > "$dir/initdb.log" 2>&1 \
		|| { echo "initdb failed"; tail -20 "$dir/initdb.log"; return 1; }
	{
		echo "unix_socket_directories = '$dir/sock'"
		echo "listen_addresses = ''"
		echo "fsync = off"
		echo "max_prepared_transactions = 10"
		echo "allow_in_place_tablespaces = on"
		local s
		for s in "$@"; do echo "$s"; done
	} >> "$dir/data/postgresql.conf"
}

server_start() {
	"$BINDIR/pg_ctl" -D "$1/data" -l "$1/log" -w -t 120 start > /dev/null 2>&1
}

server_stop() {
	"$BINDIR/pg_ctl" -D "$1/data" -m "${2:-fast}" -w stop > /dev/null 2>&1
}

q() {
	"$BINDIR/psql" -X -q -At -h "$1/sock" -U postgres -d "$2" -c "$3" 2>&1
}

# The server log's errors of the kinds a leg must not leave: assertions,
# crashes, PANICs.
log_failures() {
	grep -E "TRAP:|PANIC|terminated by signal|server process .* was terminated" "$1/log" 2> /dev/null
}
