/*
 * CH0 spike (ch_table_engines.md §5, CH0): loads libchte_engine.so the way the extension
 * will — dlopen into a plain C process built by the server image's compiler — and checks
 * that ClickHouse's own code writes parts from batches, reads them back, merges them
 * (MergeTree and ReplacingMergeTree), keeps the _pg_rowid projection through a merge, and
 * starts no thread after chte_init.
 *
 *   spike <libchte_engine.so> <empty data directory>
 */
#include <dlfcn.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "chte_engine.h"

#define API(name) static __typeof__(name) *p_##name
API(chte_init);
API(chte_shutdown);
API(chte_thread_count);
API(chte_table_open);
API(chte_table_close);
API(chte_write_begin);
API(chte_write_batch);
API(chte_write_finish);
API(chte_write_part);
API(chte_write_commit);
API(chte_write_close);
API(chte_read_begin);
API(chte_read_next);
API(chte_read_vector);
API(chte_read_end);
API(chte_merge);
API(chte_drop_part);

static chte_error err;
static int	failures;

#define CHECK(call) do { if ((call) != 0) { fprintf(stderr, "FAIL %s:%d %s: [%d] %s\n", __FILE__, __LINE__, #call, err.code, err.message); exit(1); } } while (0)
#define EXPECT(cond, ...) do { if (!(cond)) { fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); failures++; } } while (0)

static double
now(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec + ts.tv_nsec / 1e9;
}

static void
on_log(void *arg, int level, const char *message)
{
	(void) arg;
	fprintf(stderr, "  log[%d] %s\n", level, message);
}

static int	threads;

static void
expect_threads(const char *when)
{
	int			n = p_chte_thread_count();

	printf("threads %s: %d\n", when, n);
	EXPECT(n == threads, "thread count changed %s: %d -> %d", when, threads, n);
}

/* Writes rows [first, first + n) of the test data as one part numbered block. */
static void
write_part(chte_table *t, uint64_t block, uint64_t first, uint32_t n, uint32_t stride, chte_part_info *info)
{
	uint64_t   *id = malloc(n * sizeof(uint64_t));
	int32_t    *v = malloc(n * sizeof(int32_t));
	uint64_t   *offs = malloc((n + 1) * sizeof(uint64_t));
	char	   *chars = malloc((size_t) n * 24);
	int64_t    *nv = malloc(n * sizeof(int64_t));
	uint8_t    *nulls = malloc(n);
	uint64_t   *rowid = malloc(n * sizeof(uint64_t));
	chte_writer *w;
	size_t		nparts;
	double		t0 = now();

	offs[0] = 0;
	for (uint32_t i = 0; i < n; i++)
	{
		uint64_t	k = (uint64_t) i * stride % n;	/* a permutation: rows arrive unsorted */

		id[i] = first + k;
		v[i] = (int32_t) (first + k);
		offs[i + 1] = offs[i] + (uint64_t) sprintf(chars + offs[i], "s%" PRIu64, first + k);
		nulls[i] = (first + k) % 10 == 0;
		nv[i] = nulls[i] ? 0 : (int64_t) (first + k) * 3;
		rowid[i] = first + i + 1;
	}

	chte_vector cols[5] = {
		{id, NULL, NULL}, {v, NULL, NULL}, {chars, offs, NULL}, {nv, NULL, nulls}, {rowid, NULL, NULL}
	};

	CHECK(p_chte_write_begin(t, block, &w, &err));
	CHECK(p_chte_write_batch(w, n / 2, cols, &err));
	{
		/* the second half as a second batch, to exercise appending */
		uint32_t	h = n / 2;
		chte_vector rest[5] = {
			{id + h, NULL, NULL}, {v + h, NULL, NULL}, {chars, offs + h, NULL}, {nv + h, NULL, nulls + h},
			{rowid + h, NULL, NULL}
		};

		CHECK(p_chte_write_batch(w, n - h, rest, &err));
	}
	CHECK(p_chte_write_finish(w, &nparts, &err));
	EXPECT(nparts == 1, "one part expected, got %zu", nparts);
	CHECK(p_chte_write_part(w, 0, info, &err));
	CHECK(p_chte_write_commit(w, &err));
	p_chte_write_close(w);
	printf("wrote %s: %" PRIu64 " rows, %" PRIu64 " bytes, %" PRIu64 " marks in %.3f s\n",
		   info->name, info->rows, info->bytes_on_disk, info->marks, now() - t0);
	free(id);
	free(v);
	free(offs);
	free(chars);
	free(nv);
	free(nulls);
	free(rowid);
}

/* Reads a part of the test table back and checks every row. */
static void
check_part(chte_table *t, const char *part, uint64_t first, uint64_t n, int check_rowids)
{
	chte_reader *r;
	uint32_t	rows;
	uint64_t	seen = 0;
	uint64_t	prev = 0;
	double		t0 = now();

	CHECK(p_chte_read_begin(t, part, "id, v, s, n, _pg_rowid, _part_offset", &r, &err));
	for (;;)
	{
		chte_vector id, v, s, nv, rowid, off;

		CHECK(p_chte_read_next(r, &rows, &err));
		if (rows == 0)
			break;
		CHECK(p_chte_read_vector(r, 0, &id, &err));
		CHECK(p_chte_read_vector(r, 1, &v, &err));
		CHECK(p_chte_read_vector(r, 2, &s, &err));
		CHECK(p_chte_read_vector(r, 3, &nv, &err));
		CHECK(p_chte_read_vector(r, 4, &rowid, &err));
		CHECK(p_chte_read_vector(r, 5, &off, &err));
		for (uint32_t i = 0; i < rows; i++, seen++)
		{
			uint64_t	k = ((const uint64_t *) id.values)[i];
			char		expect[32];
			int			len = sprintf(expect, "s%" PRIu64, k);
			uint64_t	so = s.offsets[i];

			if (seen > 0 && k <= prev)
			{
				EXPECT(0, "%s: not sorted at row %" PRIu64 ": %" PRIu64 " after %" PRIu64, part, seen, k, prev);
				return;
			}
			prev = k;
			EXPECT(k == first + seen, "%s: row %" PRIu64 " has id %" PRIu64, part, seen, k);
			EXPECT(((const int32_t *) v.values)[i] == (int32_t) k, "%s: v of %" PRIu64, part, k);
			EXPECT(s.offsets[i + 1] - so == (uint64_t) len &&
				   memcmp((const char *) s.values + so, expect, len) == 0, "%s: s of %" PRIu64, part, k);
			EXPECT(nv.nulls && nv.nulls[i] == (k % 10 == 0), "%s: null flag of %" PRIu64, part, k);
			if (!(k % 10 == 0))
				EXPECT(((const int64_t *) nv.values)[i] == (int64_t) k * 3, "%s: n of %" PRIu64, part, k);
			EXPECT(((const uint64_t *) off.values)[i] == seen, "%s: _part_offset of row %" PRIu64, part, seen);
			if (check_rowids)
				EXPECT(((const uint64_t *) rowid.values)[i] != 0, "%s: _pg_rowid of %" PRIu64, part, k);
			if (failures > 20)
				exit(1);
		}
	}
	p_chte_read_end(r);
	EXPECT(seen == n, "%s: %" PRIu64 " rows read, %" PRIu64 " expected", part, seen, n);
	printf("read %s: %" PRIu64 " rows in %.3f s\n", part, seen, now() - t0);
}

static int
dir_exists(const char *path)
{
	struct stat st;

	return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static void
replacing(const char *data)
{
	chte_table_def def = {
		"replacing", "replacing/",
		"CREATE TABLE r (k UInt64, ver UInt32, x String) ENGINE = ReplacingMergeTree(ver) ORDER BY k"
	};
	chte_table *t;
	chte_part_info p1, p2, merged;
	const char *names[2];
	chte_reader *r;
	uint32_t	rows;
	uint64_t	seen = 0;

	CHECK(p_chte_table_open(&def, &t, &err));
	for (int part = 0; part < 2; part++)
	{
		uint64_t	k[4] = {1, 2, 3, 4};
		uint32_t	ver[4] = {10, 10, 10, 10};
		char		chars[16];
		uint64_t	offs[5] = {0, 1, 2, 3, 4};
		chte_writer *w;
		size_t		n;

		/* part 2 has newer versions of keys 2 and 3, an older one of key 4 */
		if (part == 1)
		{
			ver[1] = 20;
			ver[2] = 30;
			ver[3] = 5;
		}
		memcpy(chars, part == 0 ? "aaaa" : "bbbb", 4);
		chte_vector cols[3] = {{k, NULL, NULL}, {ver, NULL, NULL}, {chars, offs, NULL}};

		CHECK(p_chte_write_begin(t, part + 1, &w, &err));
		CHECK(p_chte_write_batch(w, 4, cols, &err));
		CHECK(p_chte_write_finish(w, &n, &err));
		CHECK(p_chte_write_part(w, 0, part == 0 ? &p1 : &p2, &err));
		CHECK(p_chte_write_commit(w, &err));
		p_chte_write_close(w);
	}
	names[0] = p1.name;
	names[1] = p2.name;
	CHECK(p_chte_merge(t, names, 2, &merged, &err));
	printf("replacing: merged %s + %s -> %s (%" PRIu64 " rows)\n", p1.name, p2.name, merged.name, merged.rows);
	EXPECT(merged.rows == 4, "ReplacingMergeTree kept %" PRIu64 " rows", merged.rows);
	CHECK(p_chte_read_begin(t, merged.name, "k, ver, x", &r, &err));
	while (p_chte_read_next(r, &rows, &err) == 0 && rows > 0)
	{
		chte_vector k, ver, x;
		/* expected winners: k1 v10 a (equal versions: the later part wins -> b), k2 v20 b, k3 v30 b, k4 v10 a */
		static const uint32_t want_ver[4] = {10, 20, 30, 10};
		static const char want_x[4] = {'b', 'b', 'b', 'a'};

		CHECK(p_chte_read_vector(r, 0, &k, &err));
		CHECK(p_chte_read_vector(r, 1, &ver, &err));
		CHECK(p_chte_read_vector(r, 2, &x, &err));
		for (uint32_t i = 0; i < rows; i++, seen++)
		{
			uint64_t	key = ((const uint64_t *) k.values)[i];

			EXPECT(key == seen + 1, "replacing: key %" PRIu64 " at %" PRIu64, key, seen);
			if (key >= 1 && key <= 4)
			{
				EXPECT(((const uint32_t *) ver.values)[i] == want_ver[key - 1], "replacing: ver of %" PRIu64, key);
				EXPECT(((const char *) x.values)[x.offsets[i]] == want_x[key - 1], "replacing: x of %" PRIu64, key);
			}
		}
	}
	p_chte_read_end(r);
	EXPECT(seen == 4, "replacing: read %" PRIu64 " rows", seen);
	p_chte_table_close(t);
	(void) data;
}

int
main(int argc, char **argv)
{
	const uint32_t n = 300000;
	void	   *lib;
	chte_table *t;
	chte_part_info p1, p2, merged;
	const char *names[2];
	char		path[4096];
	double		t0;

	if (argc != 3)
	{
		fprintf(stderr, "usage: %s <libchte_engine.so> <data directory>\n", argv[0]);
		return 2;
	}
	setvbuf(stdout, NULL, _IOLBF, 0);
	t0 = now();
	lib = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
	if (!lib)
	{
		fprintf(stderr, "FAIL dlopen: %s\n", dlerror());
		return 1;
	}
#define LOAD(name) do { p_##name = (__typeof__(p_##name)) dlsym(lib, #name); if (!p_##name) { fprintf(stderr, "FAIL dlsym %s\n", #name); return 1; } } while (0)
	LOAD(chte_init);
	LOAD(chte_shutdown);
	LOAD(chte_thread_count);
	LOAD(chte_table_open);
	LOAD(chte_table_close);
	LOAD(chte_write_begin);
	LOAD(chte_write_batch);
	LOAD(chte_write_finish);
	LOAD(chte_write_part);
	LOAD(chte_write_commit);
	LOAD(chte_write_close);
	LOAD(chte_read_begin);
	LOAD(chte_read_next);
	LOAD(chte_read_vector);
	LOAD(chte_read_end);
	LOAD(chte_merge);
	LOAD(chte_drop_part);
	printf("dlopen: %.3f s, threads %d\n", now() - t0, p_chte_thread_count());

	snprintf(path, sizeof(path), "%s/", argv[2]);
	{
		char		work[4200];
		chte_init_options opts;

		snprintf(work, sizeof(work), "%s/clickhouse/", argv[2]);
		memset(&opts, 0, sizeof(opts));
		opts.abi_version = CHTE_ABI_VERSION;
		opts.data_path = path;
		opts.work_path = work;
		opts.log_level = getenv("SPIKE_LOG") ? getenv("SPIKE_LOG") : "warning";
		opts.callbacks.log = on_log;

		t0 = now();
		CHECK(p_chte_init(&opts, &err));
		threads = p_chte_thread_count();
		printf("init: %.3f s, threads %d\n", now() - t0, threads);
	}

	{
		chte_table_def def = {
			"spike", "spike/",
			"CREATE TABLE spike (id UInt64, v Int32, s String, n Nullable(Int64), "
			"_pg_rowid UInt64 CODEC(T64, LZ4), "
			"PROJECTION _pg_rowid_index (SELECT _pg_rowid, _part_offset ORDER BY _pg_rowid)) "
			"ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192"
		};

		CHECK(p_chte_table_open(&def, &t, &err));
	}
	expect_threads("after table open");

	write_part(t, 1, 0, n, 7919, &p1);
	write_part(t, 2, n, n, 104729, &p2);
	expect_threads("after writes");
	check_part(t, p1.name, 0, n, 1);
	check_part(t, p2.name, n, n, 1);

	names[0] = p1.name;
	names[1] = p2.name;
	t0 = now();
	CHECK(p_chte_merge(t, names, 2, &merged, &err));
	printf("merged %s + %s -> %s: %" PRIu64 " rows, %" PRIu64 " bytes in %.3f s\n",
		   p1.name, p2.name, merged.name, merged.rows, merged.bytes_on_disk, now() - t0);
	EXPECT(merged.rows == 2 * n, "merged rows %" PRIu64, merged.rows);
	check_part(t, merged.name, 0, 2 * (uint64_t) n, 1);
	snprintf(path, sizeof(path), "%s/spike/%s/_pg_rowid_index.proj", argv[2], merged.name);
	EXPECT(dir_exists(path), "projection directory %s missing after merge", path);
	expect_threads("after merge");

	CHECK(p_chte_drop_part(t, p1.name, &err));
	CHECK(p_chte_drop_part(t, p2.name, &err));
	snprintf(path, sizeof(path), "%s/spike/%s", argv[2], p1.name);
	EXPECT(!dir_exists(path), "dropped part %s still there", p1.name);
	p_chte_table_close(t);

	replacing(argv[2]);
	expect_threads("at the end");

	t0 = now();
	p_chte_shutdown();
	printf("shutdown: %.3f s, threads %d\n", now() - t0, p_chte_thread_count());
	if (failures)
	{
		printf("spike: %d failures\n", failures);
		return 1;
	}
	printf("spike: ok\n");
	return 0;
}
