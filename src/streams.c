/*-------------------------------------------------------------------------
 *
 * streams.c
 *	  A MergeTree part's streams by granule run, as the engine library's
 *	  chte_part_streams_of() describes them for pg_accel's GPU (W6), kept in
 *	  shared memory: so that a GPU's unit of a part is named in any process
 *	  without the part's load (VC29; pg_accel's pg_vector_executor.md §3.28,
 *	  W16), which a fresh backend paid at each part of a device query -- 2 ms
 *	  of ClickBench's queries' 4 to 6 on the device.
 *
 *	  An entry is one column's streams over granules [begin, end) of a part:
 *	  each stream's kind, width, file in the part and compressed blocks there,
 *	  and the run's rows.  A part never changes while it lives, and its name
 *	  is never used again in its storage: an entry is keyed by the storage,
 *	  the part's name, rows and time of writing -- a storage's relfilenumber
 *	  may come back once its files are gone, its parts' names with it -- the
 *	  run and the column.  A column's answer refused, a part's or a file's
 *	  name too long for an entry, a temporary table's storage: nothing kept,
 *	  the library asked as before.
 *
 *	  A table of ch.device_streams entries in the main shared memory, made
 *	  where the extension is preloaded; probed in turn from a hash of the key,
 *	  under one lock, shared to find and exclusive to keep.  Full, it is
 *	  emptied and filled again: entries of parts merged away or dropped go so.
 *	  Not preloaded, nothing is kept.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

#include "chte.h"

#define CH_STREAMS_NAME		64	/* a part's name, with its NUL */
#define CH_STREAMS_FILE		32	/* a stream's file in its part, with its NUL */
#define CH_STREAMS_MAX		4	/* a column's streams: values, sizes or a null map, sparse offsets */

typedef struct ChStreamsKey
{
	Oid			dbid;
	Oid			spcid;
	RelFileNumber relnumber;
	int32		attnum;
	uint32		begin;
	uint32		end;
	int64		part_rows;
	TimestampTz part_created;
	char		part[CH_STREAMS_NAME];
} ChStreamsKey;

typedef struct ChStreamsStream
{
	int32		kind;
	int32		width;
	uint64		from;
	uint64		to;
	uint64		skip;
	char		file[CH_STREAMS_FILE];
} ChStreamsStream;

typedef struct ChStreamsSlot
{
	uint32		hash;			/* 0: empty */
	int32		nstreams;
	ChStreamsKey key;
	uint64		rows;			/* the run's granules' */
	ChStreamsStream streams[CH_STREAMS_MAX];
} ChStreamsSlot;

typedef struct ChStreamsShared
{
	LWLock	   *lock;
	uint32		nslots;			/* a power of two, twice the entries or more */
	uint32		used;
	ChStreamsSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ChStreamsShared;

/*
 * W21 (pg_accel's §3.28): a run's granules' rows -- end - begin of them, as
 * chte_part_streams_of() gives them -- kept once a run beside its columns'
 * streams, for vexec's condition cache: the device then says of each granule
 * whether a row of it passed the scan's quals.  A table of its own, an eighth
 * of ch.device_streams entries, under the streams' lock.
 */
typedef struct ChRunsKey
{
	Oid			dbid;
	Oid			spcid;
	RelFileNumber relnumber;
	uint32		begin;
	uint32		end;
	int64		part_rows;
	TimestampTz part_created;
	char		part[CH_STREAMS_NAME];
} ChRunsKey;

typedef struct ChRunsSlot
{
	uint32		hash;			/* 0: empty */
	uint32		nmarks;
	ChRunsKey	key;
	uint32		rows[CH_DEVICE_UNIT_MARKS];
} ChRunsSlot;

typedef struct ChRunsShared
{
	uint32		nslots;
	uint32		used;
	ChRunsSlot	slots[FLEXIBLE_ARRAY_MEMBER];
} ChRunsShared;

int			ch_device_streams = 8192;
int			ch_device_skip_granules = 4;
bool		ch_device_unit_ranges = true;
bool		ch_device_claim_alone = true;

static ChStreamsShared *shared = NULL;
static ChRunsShared *runs = NULL;
static uint64 found_runs = 0;
static uint64 described_runs = 0;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static uint32
slot_count(void)
{
	uint32		n = 2;

	while (n < (uint32) ch_device_streams * 2)
		n <<= 1;
	return n;
}

static Size
shared_size(void)
{
	return add_size(offsetof(ChStreamsShared, slots), mul_size(slot_count(), sizeof(ChStreamsSlot)));
}

/* W21: the runs' slots, a power of two, twice an eighth of the entries or more */
static uint32
runs_slot_count(void)
{
	uint32		n = 2;

	while (n < (uint32) Max(ch_device_streams / 8, 16) * 2)
		n <<= 1;
	return n;
}

static Size
runs_size(void)
{
	return add_size(offsetof(ChRunsShared, slots), mul_size(runs_slot_count(), sizeof(ChRunsSlot)));
}

static void
streams_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	if (ch_device_streams <= 0)
		return;
	RequestAddinShmemSpace(add_size(shared_size(), runs_size()));
	RequestNamedLWLockTranche("ch_table_engines device streams", 1);
}

static void
streams_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	if (ch_device_streams <= 0)
		return;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shared = ShmemInitStruct("ch_table_engines device streams", shared_size(), &found);
	if (!found)
	{
		shared->lock = &(GetNamedLWLockTranche("ch_table_engines device streams"))->lock;
		shared->nslots = slot_count();
		shared->used = 0;
		memset(shared->slots, 0, sizeof(ChStreamsSlot) * shared->nslots);
	}
	runs = ShmemInitStruct("ch_table_engines device runs", runs_size(), &found);
	if (!found)
	{
		runs->nslots = runs_slot_count();
		runs->used = 0;
		memset(runs->slots, 0, sizeof(ChRunsSlot) * runs->nslots);
	}
	LWLockRelease(AddinShmemInitLock);
}

/* From _PG_init, preloading: the setting, and the table with the postmaster's shared memory */
void
ch_streams_init(void)
{
	DefineCustomIntVariable("ch.device_streams",
							"The entries of MergeTree parts' streams kept in shared memory for a GPU's units.",
							"An entry is a column's streams over a run of a part's granules, as the engine "
							"library describes them for pg_accel's GPU, so that a process names the unit "
							"without loading the part; 0 keeps none.",
							&ch_device_streams,
							8192, 0, 1 << 24,
							PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	/*
	 * W21 (pg_accel's §3.28): a GPU's unit cut at a run of granules vexec's condition
	 * cache passes over only where the run is this long: measured on ClickBench's Q20
	 * to Q23 at 10M rows, hot, a unit costs the device about 0.13 ms and a granule of
	 * cached columns about 6 us, so a cut at every granule made 152 units of Q20's
	 * three parts and the device 2.5 times slower than reading everything; at 4, 16,
	 * 32 and 64 the four took 30.4, 18.9, 18.7 and 19.6 ms by ClickBench's mean, 21.6
	 * with the cache off.  W26: the unit goes on past such a run in ranges
	 * (ch.device_unit_ranges), a range costing the device far less than a unit: at 1,
	 * 2, 4, 8 and 32 the four took 15.0, 14.8, 15.0, 16.3 and 17.2 ms, 20.9 cut in
	 * units at 32
	 */
	DefineCustomIntVariable("ch.device_skip_granules",
							"The least run of granules passed over by vexec's condition cache at which a GPU's unit is cut.",
							"A shorter run is read with the unit, its rows dropped by the device's filter; a run at the "
							"start or the end of a part is always left out. 1 cuts at every granule.",
							&ch_device_skip_granules,
							4, 1, 1 << 20,
							PGC_USERSET, 0,
							NULL, NULL, NULL);
	/*
	 * W26 (pg_accel's §3.28): a GPU's unit not cut at those runs but read in
	 * ranges past them, up to its granules' most, where vexec takes a unit of
	 * several ranges
	 */
	DefineCustomBoolVariable("ch.device_unit_ranges",
							 "Whether a GPU's unit of a MergeTree part goes on past a run of granules passed over, in ranges.",
							 "On, the unit leaves such a run out of its ranges and goes on to its most granules; off, "
							 "the unit ends before the run, which then costs the device a unit more.",
							 &ch_device_unit_ranges,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	/*
	 * VC35 (pg_accel's §3.29 W32): the device owner of a parallel plan, the
	 * only participant claiming, takes a part's units at once
	 */
	DefineCustomBoolVariable("ch.device_claim_alone",
							 "Whether a parallel plan's device owner claims a part's units at once.",
							 "On, the participant that reads every unit on the device takes the rest of a part's "
							 "units with the first it claims, cut as a serial scan's; off, each of the parallel "
							 "scan's units is a device unit of its own.",
							 &ch_device_claim_alone,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = streams_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = streams_shmem_startup;
}

/* A column's key, its padding zeroed; false where no entry can hold it */
static bool
make_key(const ChTable *t, const ChPart *part, uint32 begin, uint32 end, AttrNumber attnum, ChStreamsKey *key)
{
	if (shared == NULL || t->storage.backend >= 0 || strlen(part->name) >= CH_STREAMS_NAME)
		return false;
	memset(key, 0, sizeof(*key));
	key->dbid = t->storage.locator.dbOid;
	key->spcid = t->storage.locator.spcOid;
	key->relnumber = t->storage.locator.relNumber;
	key->attnum = attnum;
	key->begin = begin;
	key->end = end;
	key->part_rows = part->rows;
	key->part_created = part->created;
	strcpy(key->part, part->name);
	return true;
}

static uint32
key_hash_bytes(const unsigned char *p, Size n)
{
	uint32		h = hash_bytes(p, n);

	return h == 0 ? 1 : h;
}

static uint32
key_hash(const ChStreamsKey *key)
{
	return key_hash_bytes((const unsigned char *) key, sizeof(*key));
}

/* The slot of a key, or of the empty one where its probe ends; under the lock */
static ChStreamsSlot *
probe(const ChStreamsKey *key, uint32 h)
{
	uint32		mask = shared->nslots - 1;

	for (uint32 i = h & mask;; i = (i + 1) & mask)
	{
		ChStreamsSlot *s = &shared->slots[i];

		if (s->hash == 0 || (s->hash == h && memcmp(&s->key, key, sizeof(*key)) == 0))
			return s;
	}
}

/* W21: a run's key -- the columns' without the attribute -- its padding zeroed; false where none holds it */
static bool
make_runs_key(const ChTable *t, const ChPart *part, uint32 begin, uint32 end, ChRunsKey *key)
{
	if (runs == NULL || t->storage.backend >= 0 || strlen(part->name) >= CH_STREAMS_NAME || end - begin > CH_DEVICE_UNIT_MARKS)
		return false;
	memset(key, 0, sizeof(*key));
	key->dbid = t->storage.locator.dbOid;
	key->spcid = t->storage.locator.spcOid;
	key->relnumber = t->storage.locator.relNumber;
	key->begin = begin;
	key->end = end;
	key->part_rows = part->rows;
	key->part_created = part->created;
	strcpy(key->part, part->name);
	return true;
}

static ChRunsSlot *
runs_probe(const ChRunsKey *key, uint32 h)
{
	uint32		mask = runs->nslots - 1;

	for (uint32 i = h & mask;; i = (i + 1) & mask)
	{
		ChRunsSlot *s = &runs->slots[i];

		if (s->hash == 0 || (s->hash == h && memcmp(&s->key, key, sizeof(*key)) == 0))
			return s;
	}
}

/*
 * The streams of the columns attnums over granules [begin, end) of a part,
 * as chte_part_streams_of() would answer for them -- each column's in its
 * order, each stream's column its place among them -- and the run's rows,
 * where every column's are kept; streams and their files palloc'd.
 */
bool
ch_streams_find(const ChTable *t, const ChPart *part, uint32 begin, uint32 end, int ncols,
				const AttrNumber *attnums, uint64 *rows, chte_part_stream **streams, size_t *nstreams,
				uint32 **granule_rows)
{
	ChStreamsKey *keys;
	uint32	   *hashes;
	chte_part_stream *out;
	size_t		n = 0;
	bool		all = true;

	if (shared == NULL || ncols <= 0)
		return false;
	keys = palloc(sizeof(ChStreamsKey) * ncols);
	hashes = palloc(sizeof(uint32) * ncols);
	for (int c = 0; c < ncols; c++)
	{
		if (!make_key(t, part, begin, end, attnums[c], &keys[c]))
		{
			pfree(keys);
			pfree(hashes);
			return false;
		}
		hashes[c] = key_hash(&keys[c]);
	}
	out = palloc0(sizeof(chte_part_stream) * ncols * CH_STREAMS_MAX);
	*granule_rows = NULL;
	LWLockAcquire(shared->lock, LW_SHARED);
	for (int c = 0; c < ncols && all; c++)
	{
		ChStreamsSlot *s = probe(&keys[c], hashes[c]);

		if (s->hash == 0)
		{
			all = false;
			break;
		}
		*rows = s->rows;
		for (int k = 0; k < s->nstreams; k++)
		{
			chte_part_stream *x = &out[n++];

			x->column = c;
			x->kind = s->streams[k].kind;
			x->width = s->streams[k].width;
			x->from = s->streams[k].from;
			x->to = s->streams[k].to;
			x->skip = s->streams[k].skip;
			strlcpy(x->file, s->streams[k].file, sizeof(x->file));
		}
	}
	/* W21: the run's granules' rows, where kept */
	if (all)
	{
		ChRunsKey	rk;

		if (make_runs_key(t, part, begin, end, &rk))
		{
			ChRunsSlot *r = runs_probe(&rk, key_hash_bytes((const unsigned char *) &rk, sizeof(rk)));

			if (r->hash != 0 && r->nmarks == end - begin)
			{
				*granule_rows = palloc(sizeof(uint32) * r->nmarks);
				memcpy(*granule_rows, r->rows, sizeof(uint32) * r->nmarks);
			}
		}
	}
	LWLockRelease(shared->lock);
	pfree(keys);
	pfree(hashes);
	if (!all)
	{
		pfree(out);
		return false;
	}
	*streams = out;
	*nstreams = n;
	found_runs++;
	return true;
}

/* The library's answer for the columns attnums over [begin, end) of a part, an entry each kept */
void
ch_streams_keep(const ChTable *t, const ChPart *part, uint32 begin, uint32 end, int ncols,
				const AttrNumber *attnums, const chte_part_streams *ps)
{
	if (shared == NULL || ps->refusal != NULL)
		return;
	/* each column's streams within an entry, and their files' names, or none kept */
	for (int c = 0; c < ncols; c++)
	{
		int			k = 0;

		for (size_t i = 0; i < ps->nstreams; i++)
			if (ps->streams[i].column == c &&
				(++k > CH_STREAMS_MAX || strlen(ps->streams[i].file) >= CH_STREAMS_FILE))
				return;
	}
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);
	/* full: every entry goes, those of parts merged away or dropped among them */
	if (shared->used + (uint32) ncols > (uint32) ch_device_streams)
	{
		memset(shared->slots, 0, sizeof(ChStreamsSlot) * shared->nslots);
		shared->used = 0;
	}
	for (int c = 0; c < ncols && shared->used < (uint32) ch_device_streams; c++)
	{
		ChStreamsKey key;
		uint32		h;
		ChStreamsSlot *s;

		if (!make_key(t, part, begin, end, attnums[c], &key))
			break;
		h = key_hash(&key);
		s = probe(&key, h);
		if (s->hash == 0)
			shared->used++;
		s->hash = h;
		s->key = key;
		s->rows = ps->rows;
		s->nstreams = 0;
		for (size_t i = 0; i < ps->nstreams; i++)
		{
			const chte_part_stream *x = &ps->streams[i];
			ChStreamsStream *y;

			if (x->column != c)
				continue;
			y = &s->streams[s->nstreams++];
			y->kind = x->kind;
			y->width = x->width;
			y->from = x->from;
			y->to = x->to;
			y->skip = x->skip;
			strlcpy(y->file, x->file, sizeof(y->file));
		}
	}
	/* W21: the run's granules' rows, once a run */
	if (ps->granule_rows != NULL && runs != NULL)
	{
		ChRunsKey	rk;

		if (make_runs_key(t, part, begin, end, &rk))
		{
			uint32		h = key_hash_bytes((const unsigned char *) &rk, sizeof(rk));
			ChRunsSlot *r;

			if (runs->used + 1 > runs->nslots / 2)
			{
				memset(runs->slots, 0, sizeof(ChRunsSlot) * runs->nslots);
				runs->used = 0;
			}
			r = runs_probe(&rk, h);
			if (r->hash == 0)
				runs->used++;
			r->hash = h;
			r->key = rk;
			r->nmarks = end - begin;
			memcpy(r->rows, ps->granule_rows, sizeof(uint32) * (end - begin));
		}
	}
	LWLockRelease(shared->lock);
}

/* A run described by the library, for the tests */
void
ch_streams_described(void)
{
	described_runs++;
}

/*
 * ch.device_streams(): the entries kept now -- NULL where none can be, the
 * extension not preloaded or ch.device_streams 0 -- this process's runs found
 * among them and described by the library, and the parts the library has
 * loaded in the process (NULL where it does not count them)
 */
PG_FUNCTION_INFO_V1(ch_device_streams_info);
Datum
ch_device_streams_info(PG_FUNCTION_ARGS)
{
	TupleDesc	desc;
	Datum		values[4];
	bool		nulls[4] = {false};

	if (get_call_result_type(fcinfo, NULL, &desc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	if (shared != NULL)
	{
		LWLockAcquire(shared->lock, LW_SHARED);
		values[0] = Int32GetDatum((int32) shared->used);
		LWLockRelease(shared->lock);
	}
	else
		nulls[0] = true;
	values[1] = Int64GetDatum((int64) found_runs);
	values[2] = Int64GetDatum((int64) described_runs);
	if (!ch_engine_loaded())
		values[3] = Int64GetDatum(0);
	else if (ch_engine()->parts_loaded != NULL)
		values[3] = Int64GetDatum((int64) ch_engine()->parts_loaded());
	else
		nulls[3] = true;
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(BlessTupleDesc(desc), values, nulls)));
}
