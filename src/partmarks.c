/*-------------------------------------------------------------------------
 *
 * partmarks.c
 *	  A MergeTree part's marks as the first process decompressed them, kept in
 *	  shared memory, so that the next process makes its mark cache's entry of
 *	  them and reads no marks file (chte_set_marks_store(); pg_accel's
 *	  pg_vector_executor.md §3.38, "After VP6, read again", VC42's marks).  A
 *	  fresh process decompressed its part's marks of each stream its scan reads:
 *	  3 to 14% of ClickBench's small queries' cycles at 100M rows, once the
 *	  parts' metadata were shared (partmeta.c), 13 parts in each of a query's 17
 *	  processes.
 *
 *	  An entry is a stream's marks of a part, plain, 16 bytes each, keyed as
 *	  partmeta.c keys its entries -- the storage, the part's name, rows and time
 *	  of writing -- and the stream's name.  A table of ch.part_marks_entries
 *	  entries in the main shared memory and a pool of ch.part_marks_bytes beside
 *	  it, under one lock, shared to find and exclusive to keep; full, both are
 *	  emptied and filled again.  The library asks as a scan's read of a part
 *	  begins, in the caller's thread, the part named by
 *	  ch_part_marks_begin(): nothing here raises an error or leaves the call,
 *	  which is ClickHouse's.  Not preloaded, nothing is kept.
 *
 *	  Off by default (ch.enable_shared_marks): measured in the timed protocol at
 *	  100M rows over the small queries, nothing with vexec's pool (cold 0.992,
 *	  hot 0.999) and hot 0.982 without it, cold 1.002: the plain marks' copy and
 *	  their packing again (MarksInCompressedFile::create()) take most of what the
 *	  file's read and decompression cost.
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

#define CH_PARTMARKS_NAME	64		/* a part's name, a stream's, with its NUL */
#define CH_PARTMARK_BYTES	16		/* MarkInCompressedFile */

typedef struct ChPartMarksKey
{
	Oid			dbid;
	Oid			spcid;
	RelFileNumber relnumber;
	int64		part_rows;
	TimestampTz part_created;
	char		part[CH_PARTMARKS_NAME];
	char		stream[CH_PARTMARKS_NAME];
} ChPartMarksKey;

typedef struct ChPartMarksSlot
{
	uint32		hash;			/* 0: empty */
	uint32		count;			/* marks */
	Size		off;			/* in the pool */
	ChPartMarksKey key;
} ChPartMarksSlot;

typedef struct ChPartMarksShared
{
	LWLock	   *lock;
	uint32		nslots;			/* a power of two, twice the entries or more */
	uint32		used;
	Size		pool_size;
	Size		pool_used;
	ChPartMarksSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ChPartMarksShared;

int			ch_part_marks_entries = 16384;
int			ch_part_marks_bytes = 64 * 1024;	/* kB */
bool		ch_enable_shared_marks = false;

static ChPartMarksShared *shared = NULL;
static const ChTable *current_table = NULL;
static const ChPart *current_part = NULL;
static uint64 found_count = 0;
static uint64 stored_count = 0;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static uint32
slot_count(void)
{
	uint32		n = 2;

	while (n < (uint32) ch_part_marks_entries * 2)
		n <<= 1;
	return n;
}

static Size
pool_bytes(void)
{
	return (Size) ch_part_marks_bytes * 1024;
}

static Size
shared_size(void)
{
	return add_size(add_size(offsetof(ChPartMarksShared, slots), mul_size(slot_count(), sizeof(ChPartMarksSlot))),
					pool_bytes());
}

static inline char *
pool(void)
{
	return (char *) &shared->slots[shared->nslots];
}

/* Every entry gone, and the pool's bytes with them; under the lock */
static void
empty_all(void)
{
	memset(shared->slots, 0, sizeof(ChPartMarksSlot) * shared->nslots);
	shared->used = 0;
	shared->pool_used = 0;
}

static void
partmarks_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	if (ch_part_marks_entries <= 0 || ch_part_marks_bytes <= 0)
		return;
	RequestAddinShmemSpace(shared_size());
	RequestNamedLWLockTranche("ch_table_engines part marks", 1);
}

static void
partmarks_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	if (ch_part_marks_entries <= 0 || ch_part_marks_bytes <= 0)
		return;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shared = ShmemInitStruct("ch_table_engines part marks", shared_size(), &found);
	if (!found)
	{
		shared->lock = &(GetNamedLWLockTranche("ch_table_engines part marks"))->lock;
		shared->nslots = slot_count();
		shared->pool_size = pool_bytes();
		empty_all();
	}
	LWLockRelease(AddinShmemInitLock);
}

/* From _PG_init, preloading: the settings, and the table with the postmaster's shared memory */
void
ch_partmarks_init(void)
{
	DefineCustomIntVariable("ch.part_marks_entries",
							"The entries of MergeTree parts' decompressed marks kept in shared memory.",
							"An entry is the marks of a stream of a part as the first process decompressed them, "
							"so that the next process reads no marks file; 0 keeps none.",
							&ch_part_marks_entries,
							16384, 0, 1 << 20,
							PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("ch.part_marks_bytes",
							"The shared memory the parts' decompressed marks are kept in.",
							"A stream's entry holds 16 bytes a granule; full, every entry goes and they are kept again.",
							&ch_part_marks_bytes,
							64 * 1024, 0, INT_MAX / 1024,
							PGC_POSTMASTER, GUC_UNIT_KB,
							NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_shared_marks",
							 "Whether a process takes a MergeTree part's marks another process decompressed.",
							 "On, a scan's marks kept in shared memory as one process decompressed them go into the "
							 "next process's mark cache; off, each process reads its marks files.",
							 &ch_enable_shared_marks,
							 false,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = partmarks_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = partmarks_shmem_startup;
}

/* An entry's key for the part being read, its padding zeroed; false where none can hold it */
static bool
make_key(const char *stream, ChPartMarksKey *key)
{
	const ChTable *t = current_table;
	const ChPart *part = current_part;

	if (shared == NULL || !ch_enable_shared_marks || t == NULL || part == NULL || t->storage.backend >= 0 ||
		part->listing != NULL || strlen(part->name) >= CH_PARTMARKS_NAME || strlen(stream) >= CH_PARTMARKS_NAME)
		return false;
	memset(key, 0, sizeof(*key));
	key->dbid = t->storage.locator.dbOid;
	key->spcid = t->storage.locator.spcOid;
	key->relnumber = t->storage.locator.relNumber;
	key->part_rows = part->rows;
	key->part_created = part->created;
	strcpy(key->part, part->name);
	strcpy(key->stream, stream);
	return true;
}

static uint32
key_hash(const ChPartMarksKey *key)
{
	uint32		h = hash_bytes((const unsigned char *) key, sizeof(*key));

	return h == 0 ? 1 : h;
}

/* The slot of a key, or of the empty one where its probe ends; under the lock */
static ChPartMarksSlot *
probe(const ChPartMarksKey *key, uint32 h)
{
	uint32		mask = shared->nslots - 1;

	for (uint32 i = h & mask;; i = (i + 1) & mask)
	{
		ChPartMarksSlot *s = &shared->slots[i];

		if (s->hash == 0 || (s->hash == h && memcmp(&s->key, key, sizeof(*key)) == 0))
			return s;
	}
}

/* chte_marks_store's find: the stream's `count` marks copied, where an entry of as many holds them */
static int
marks_find(void *arg, const char *stream, size_t count, void *into)
{
	ChPartMarksKey key;
	ChPartMarksSlot *s;
	uint32		h;
	int			found = 0;

	if (!make_key(stream, &key))
		return 0;
	h = key_hash(&key);
	LWLockAcquire(shared->lock, LW_SHARED);
	s = probe(&key, h);
	if (s->hash != 0 && s->count == count)
	{
		memcpy(into, pool() + s->off, count * CH_PARTMARK_BYTES);
		found = 1;
	}
	LWLockRelease(shared->lock);
	if (found)
		found_count++;
	return found;
}

/* chte_marks_store's keep: the stream's marks as this process decompressed them */
static void
marks_keep(void *arg, const char *stream, size_t count, const void *marks)
{
	ChPartMarksKey key;
	ChPartMarksSlot *s;
	uint32		h;
	Size		need = count * CH_PARTMARK_BYTES;

	if (!make_key(stream, &key) || count == 0 || need > shared->pool_size || count > PG_UINT32_MAX)
		return;
	h = key_hash(&key);
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);
	/* full, the table or the pool: every entry goes, those of parts merged away or dropped among them */
	if (shared->used + 1 > (uint32) ch_part_marks_entries || shared->pool_used + need > shared->pool_size)
		empty_all();
	s = probe(&key, h);
	if (s->hash == 0)
	{
		shared->used++;
		s->hash = h;
		s->count = (uint32) count;
		s->off = shared->pool_used;
		s->key = key;
		memcpy(pool() + s->off, marks, need);
		shared->pool_used += need;
		stored_count++;
	}
	LWLockRelease(shared->lock);
}

/* The store handed to the library as it starts; nothing where no process keeps any */
void
ch_part_marks_attach(const ChEngine *eng)
{
	static const chte_marks_store store = {NULL, marks_find, marks_keep};

	if (shared != NULL && eng->set_marks_store != NULL)
		eng->set_marks_store(&store);
}

/* The part whose marks the library will ask for, as a scan's read of it begins */
void
ch_part_marks_begin(const ChTable *t, const ChPart *part)
{
	current_table = t;
	current_part = part;
}

/*
 * ch.part_marks(): the entries kept and their bytes, and the session's process's
 * finds and keeps -- NULLs but the last two where none is kept (not preloaded, or
 * the settings 0)
 */
PG_FUNCTION_INFO_V1(ch_part_marks_info);
Datum
ch_part_marks_info(PG_FUNCTION_ARGS)
{
	TupleDesc	desc;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};

	if (get_call_result_type(fcinfo, NULL, &desc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	if (shared == NULL)
		nulls[0] = nulls[1] = true;
	else
	{
		LWLockAcquire(shared->lock, LW_SHARED);
		values[0] = Int32GetDatum((int32) shared->used);
		values[1] = Int64GetDatum((int64) shared->pool_used);
		LWLockRelease(shared->lock);
	}
	values[2] = Int64GetDatum((int64) found_count);
	values[3] = Int64GetDatum((int64) stored_count);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(BlessTupleDesc(desc), values, nulls)));
}
