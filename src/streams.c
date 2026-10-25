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

int			ch_device_streams = 8192;

static ChStreamsShared *shared = NULL;
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

static void
streams_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	if (ch_device_streams <= 0)
		return;
	RequestAddinShmemSpace(shared_size());
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
key_hash(const ChStreamsKey *key)
{
	uint32		h = hash_bytes((const unsigned char *) key, sizeof(*key));

	return h == 0 ? 1 : h;
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

/*
 * The streams of the columns attnums over granules [begin, end) of a part,
 * as chte_part_streams_of() would answer for them -- each column's in its
 * order, each stream's column its place among them -- and the run's rows,
 * where every column's are kept; streams and their files palloc'd.
 */
bool
ch_streams_find(const ChTable *t, const ChPart *part, uint32 begin, uint32 end, int ncols,
				const AttrNumber *attnums, uint64 *rows, chte_part_stream **streams, size_t *nstreams)
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
