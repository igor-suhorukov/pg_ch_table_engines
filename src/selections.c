/*-------------------------------------------------------------------------
 *
 * selections.c
 *	  The granules a filter keeps of a MergeTree part, as the engine library's
 *	  chte_part_select() selects them, kept in shared memory: so that a
 *	  process plans and claims a part under a filter its summary cannot decide
 *	  -- the filter's key keeps some of the part's granules, VC20's B7 says
 *	  neither all nor none -- without loading the part (VC31; pg_accel's
 *	  pg_vector_executor.md §3.28, W24).  A fresh backend loaded each such part
 *	  while it planned, its columns, checksums and primary index, about 1.2 ms
 *	  a part: ClickBench's Q19 and Q36 to Q42 planned in 4.1 to 5.7 ms, where
 *	  the queries its summaries decide took 1.8 to 2.2.
 *
 *	  An entry is a part's answer for one filter over marks [begin, end): the
 *	  ranges of granules kept and their rows.  A part never changes while it
 *	  lives, and its name is never used again in its storage: as the streams'
 *	  (streams.c), an entry is keyed by the storage, the part's name, rows and
 *	  time of writing, and the marks asked for -- and by the filter's key
 *	  (ch_filter_make_keys()), two hashes of the filter's nodes and values,
 *	  the table's definition and the process's engine settings, which are
 *	  what else the answer depends on.  A filter of a hash join's keys
 *	  (VC3), whose values change with each batch of the join's inner side,
 *	  has no key, and its answers are not kept; nor those of more ranges than
 *	  an entry holds, nor a temporary table's.
 *
 *	  A table of ch.part_selections entries in the main shared memory, made
 *	  where the extension is preloaded; probed from a hash of the key, under
 *	  one lock, shared to find and exclusive to keep.  Full, it is emptied
 *	  and filled again: entries of parts merged away or dropped go so.  Not
 *	  preloaded, nothing is kept.  With CHTE_SPEC_CHECK in the environment,
 *	  the tests', each answer found is compared with the engine's.
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

#define CH_SELECTIONS_NAME	64		/* a part's name, with its NUL */

typedef struct ChSelectionsKey
{
	Oid			dbid;
	Oid			spcid;
	RelFileNumber relnumber;
	uint32		begin;
	uint32		end;
	int64		part_rows;
	TimestampTz part_created;
	uint64		filter_h1;
	uint64		filter_h2;
	char		part[CH_SELECTIONS_NAME];
} ChSelectionsKey;

typedef struct ChSelectionsSlot
{
	uint32		hash;			/* 0: empty */
	uint32		nranges;
	ChSelectionsKey key;
	uint64		rows;			/* the kept granules' */
	chte_mark_range ranges[CH_SELECTION_RANGES];
} ChSelectionsSlot;

typedef struct ChSelectionsShared
{
	LWLock	   *lock;
	uint32		nslots;			/* a power of two, twice the entries or more */
	uint32		used;
	ChSelectionsSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ChSelectionsShared;

int			ch_part_selections = 4096;
bool		ch_enable_part_selections = true;

static ChSelectionsShared *shared = NULL;
static uint64 found_count = 0;
static uint64 stored_count = 0;
static uint64 checked_count = 0;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static uint32
slot_count(void)
{
	uint32		n = 2;

	while (n < (uint32) ch_part_selections * 2)
		n <<= 1;
	return n;
}

static Size
shared_size(void)
{
	return add_size(offsetof(ChSelectionsShared, slots), mul_size(slot_count(), sizeof(ChSelectionsSlot)));
}

static void
selections_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	if (ch_part_selections <= 0)
		return;
	RequestAddinShmemSpace(shared_size());
	RequestNamedLWLockTranche("ch_table_engines part selections", 1);
}

static void
selections_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	if (ch_part_selections <= 0)
		return;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shared = ShmemInitStruct("ch_table_engines part selections", shared_size(), &found);
	if (!found)
	{
		shared->lock = &(GetNamedLWLockTranche("ch_table_engines part selections"))->lock;
		shared->nslots = slot_count();
		shared->used = 0;
		memset(shared->slots, 0, sizeof(ChSelectionsSlot) * shared->nslots);
	}
	LWLockRelease(AddinShmemInitLock);
}

/* From _PG_init, preloading: the settings, and the table with the postmaster's shared memory */
void
ch_selections_init(void)
{
	DefineCustomIntVariable("ch.part_selections",
							"The entries of MergeTree parts' granules a filter keeps, kept in shared memory.",
							"An entry is a part's granules a filter keeps, as the engine library selects them, so "
							"that a process plans and claims the part under that filter without loading it; "
							"0 keeps none.",
							&ch_part_selections,
							4096, 0, 1 << 22,
							PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = selections_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = selections_shmem_startup;
}

/* An entry's key, its padding zeroed; false where none can hold it */
static bool
make_key(const ChTable *t, const ChPart *part, const ChFilterKey *fk, uint32 begin, uint32 end,
		 ChSelectionsKey *key)
{
	if (shared == NULL || !ch_enable_part_selections || fk == NULL || (fk->h1 == 0 && fk->h2 == 0) ||
		t->storage.backend >= 0 || part->listing != NULL || strlen(part->name) >= CH_SELECTIONS_NAME)
		return false;
	memset(key, 0, sizeof(*key));
	key->dbid = t->storage.locator.dbOid;
	key->spcid = t->storage.locator.spcOid;
	key->relnumber = t->storage.locator.relNumber;
	key->begin = begin;
	key->end = end;
	key->part_rows = part->rows;
	key->part_created = part->created;
	key->filter_h1 = fk->h1;
	key->filter_h2 = fk->h2;
	strcpy(key->part, part->name);
	return true;
}

static uint32
key_hash(const ChSelectionsKey *key)
{
	uint32		h = hash_bytes((const unsigned char *) key, sizeof(*key));

	return h == 0 ? 1 : h;
}

/* The slot of a key, or of the empty one where its probe ends; under the lock */
static ChSelectionsSlot *
probe(const ChSelectionsKey *key, uint32 h)
{
	uint32		mask = shared->nslots - 1;

	for (uint32 i = h & mask;; i = (i + 1) & mask)
	{
		ChSelectionsSlot *s = &shared->slots[i];

		if (s->hash == 0 || (s->hash == h && memcmp(&s->key, key, sizeof(*key)) == 0))
			return s;
	}
}

/*
 * The granules of marks [begin, end) of a part the filter of key fk keeps,
 * where an entry holds them: as chte_part_select() answers, at most cap of
 * the ranges into ranges[], their count into *nranges and their rows into
 * *rows.
 */
bool
ch_selections_find(const ChTable *t, const ChPart *part, const ChFilterKey *fk, uint32 begin, uint32 end,
				   chte_mark_range *ranges, size_t cap, size_t *nranges, uint64 *rows)
{
	ChSelectionsKey key;
	uint32		h;
	ChSelectionsSlot *s;
	bool		found = false;

	if (!make_key(t, part, fk, begin, end, &key))
		return false;
	h = key_hash(&key);
	LWLockAcquire(shared->lock, LW_SHARED);
	s = probe(&key, h);
	if (s->hash != 0)
	{
		found = true;
		*nranges = s->nranges;
		*rows = s->rows;
		if (ranges != NULL)
			memcpy(ranges, s->ranges, sizeof(chte_mark_range) * Min((size_t) s->nranges, cap));
	}
	LWLockRelease(shared->lock);
	if (found)
		found_count++;
	return found;
}

/* The engine library's answer for marks [begin, end) of a part under the filter of key fk, kept */
void
ch_selections_keep(const ChTable *t, const ChPart *part, const ChFilterKey *fk, uint32 begin, uint32 end,
				   const chte_mark_range *ranges, size_t nranges, uint64 rows)
{
	ChSelectionsKey key;
	uint32		h;
	ChSelectionsSlot *s;

	if (nranges > CH_SELECTION_RANGES || !make_key(t, part, fk, begin, end, &key))
		return;
	h = key_hash(&key);
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);
	/* full: every entry goes, those of parts merged away or dropped among them */
	if (shared->used + 1 > (uint32) ch_part_selections)
	{
		memset(shared->slots, 0, sizeof(ChSelectionsSlot) * shared->nslots);
		shared->used = 0;
	}
	s = probe(&key, h);
	if (s->hash == 0)
		shared->used++;
	s->hash = h;
	s->key = key;
	s->rows = rows;
	s->nranges = (uint32) nranges;
	memcpy(s->ranges, ranges, sizeof(chte_mark_range) * nranges);
	LWLockRelease(shared->lock);
	stored_count++;
}

/*
 * CHTE_SPEC_CHECK: an answer found compared with the engine's, the
 * statement's error where they differ
 */
void
ch_selections_check(const ChPart *part, uint32 begin, uint32 end, const chte_mark_range *found_ranges,
					size_t found_n, uint64 found_rows, const chte_mark_range *ranges, size_t n, uint64 rows)
{
	checked_count++;
	if (found_n != n || found_rows != rows ||
		(n > 0 && memcmp(found_ranges, ranges, sizeof(chte_mark_range) * n) != 0))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("the granules kept of part \"%s\", marks %u to %u, are not the engine library's",
						part->name, begin, end),
				 errdetail("Kept: %zu ranges of " UINT64_FORMAT " rows; the library's: %zu ranges of " UINT64_FORMAT
						   " rows.", found_n, found_rows, n, rows)));
}

/*
 * ch.part_selections(): the entries kept now -- NULL where none can be, the
 * extension not preloaded or ch.part_selections 0 -- and this process's
 * answers found among them, kept, and checked against the library's
 * (CHTE_SPEC_CHECK)
 */
PG_FUNCTION_INFO_V1(ch_part_selections_info);
Datum
ch_part_selections_info(PG_FUNCTION_ARGS)
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
	values[1] = Int64GetDatum((int64) found_count);
	values[2] = Int64GetDatum((int64) stored_count);
	values[3] = Int64GetDatum((int64) checked_count);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(BlessTupleDesc(desc), values, nulls)));
}
