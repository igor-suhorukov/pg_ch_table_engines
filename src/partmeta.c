/*-------------------------------------------------------------------------
 *
 * partmeta.c
 *	  A MergeTree part's metadata as the first process loaded it -- its
 *	  columns, serialization, checksums, index granularity, counts and
 *	  default codec, the engine library's blob of them
 *	  (chte_part_metadata_export()) -- kept in shared memory, so that the
 *	  next process makes its part of the blob and parses none of the part's
 *	  metadata files (chte_part_metadata_import(); pg_accel's
 *	  pg_vector_executor.md §3.38, "After VP5, read again", VC42).  A fresh
 *	  process loaded each part it read: columns.txt's types through
 *	  ClickHouse's SQL parser, serialization.json through Poco's JSON
 *	  parser, the checksums and the granularity -- 13 to 24% of ClickBench's
 *	  small queries' cycles at 100M rows, 13 parts in each of a query's 17
 *	  processes.
 *
 *	  An entry is a part's blob, keyed as selections.c keys its entries --
 *	  the storage, the part's name, rows and time of writing -- a part never
 *	  changing while it lives and its name never used again in its storage.
 *	  A table of ch.part_metadata_entries entries in the main shared memory
 *	  and a pool of ch.part_metadata_bytes for the blobs beside it, under one
 *	  lock, shared to find and exclusive to keep; full, both are emptied and
 *	  filled again, the entries of parts merged away or dropped going so.  A
 *	  part of the pages store is prepared as it is first attached in the
 *	  process (ch_pages_attach()): its blob handed to the library where an
 *	  entry holds it, else the part loaded now -- the library is about to
 *	  read it -- and its blob kept.  Not preloaded, nothing is kept.
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

#define CH_PARTMETA_NAME	64		/* a part's name, with its NUL */

typedef struct ChPartMetaKey
{
	Oid			dbid;
	Oid			spcid;
	RelFileNumber relnumber;
	int64		part_rows;
	TimestampTz part_created;
	char		part[CH_PARTMETA_NAME];
} ChPartMetaKey;

/*
 * VC46: a column's rows and exact default rows in a part, as the library counted
 * them, kept with its blob, by attribute number
 */
typedef struct ChPartMetaFigure
{
	int32		attno;
	uint64		rows;
	uint64		defaults;
} ChPartMetaFigure;

typedef struct ChPartMetaSlot
{
	uint32		hash;			/* 0: empty */
	uint32		len;
	Size		off;			/* in the pool */
	uint32		nfigures;		/* ChPartMetaFigure, after the blob's bytes */
	ChPartMetaKey key;
} ChPartMetaSlot;

typedef struct ChPartMetaShared
{
	LWLock	   *lock;
	uint32		nslots;			/* a power of two, twice the entries or more */
	uint32		used;
	Size		pool_size;
	Size		pool_used;
	ChPartMetaSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ChPartMetaShared;

int			ch_part_metadata_entries = 4096;
int			ch_part_metadata_bytes = 64 * 1024;	/* kB */
bool		ch_enable_shared_part_metadata = true;

static ChPartMetaShared *shared = NULL;
static uint64 found_count = 0;
static uint64 stored_count = 0;
static uint64 taken_count = 0;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static uint32
slot_count(void)
{
	uint32		n = 2;

	while (n < (uint32) ch_part_metadata_entries * 2)
		n <<= 1;
	return n;
}

static Size
pool_bytes(void)
{
	return (Size) ch_part_metadata_bytes * 1024;
}

static Size
shared_size(void)
{
	return add_size(add_size(offsetof(ChPartMetaShared, slots), mul_size(slot_count(), sizeof(ChPartMetaSlot))),
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
	memset(shared->slots, 0, sizeof(ChPartMetaSlot) * shared->nslots);
	shared->used = 0;
	shared->pool_used = 0;
}

static void
partmeta_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	if (ch_part_metadata_entries <= 0 || ch_part_metadata_bytes <= 0)
		return;
	RequestAddinShmemSpace(shared_size());
	RequestNamedLWLockTranche("ch_table_engines part metadata", 1);
}

static void
partmeta_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	if (ch_part_metadata_entries <= 0 || ch_part_metadata_bytes <= 0)
		return;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shared = ShmemInitStruct("ch_table_engines part metadata", shared_size(), &found);
	if (!found)
	{
		shared->lock = &(GetNamedLWLockTranche("ch_table_engines part metadata"))->lock;
		shared->nslots = slot_count();
		shared->pool_size = pool_bytes();
		empty_all();
	}
	LWLockRelease(AddinShmemInitLock);
}

/* From _PG_init, preloading: the settings, and the table with the postmaster's shared memory */
void
ch_partmeta_init(void)
{
	DefineCustomIntVariable("ch.part_metadata_entries",
							"The entries of MergeTree parts' loaded metadata kept in shared memory.",
							"An entry is a part's metadata as the first process loaded it, so that the next "
							"process makes its part without reading and parsing the part's metadata files; 0 "
							"keeps none.",
							&ch_part_metadata_entries,
							4096, 0, 1 << 20,
							PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("ch.part_metadata_bytes",
							"The shared memory the parts' loaded metadata are kept in.",
							"A part's entry holds its columns, serialization, checksums and index granularity, "
							"some tens of kB; full, every entry goes and they are kept again.",
							&ch_part_metadata_bytes,
							64 * 1024, 0, INT_MAX / 1024,
							PGC_POSTMASTER, GUC_UNIT_KB,
							NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_shared_part_metadata",
							 "Whether a process makes a MergeTree part of the metadata another process loaded.",
							 "On, a part's metadata kept in shared memory as one process loaded it is handed to "
							 "the engine library as the part is first attached; off, each process reads and "
							 "parses the part's files.",
							 &ch_enable_shared_part_metadata,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = partmeta_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = partmeta_shmem_startup;
}

/* An entry's key, its padding zeroed; false where none can hold it */
static bool
make_key(const ChTable *t, const ChPart *part, ChPartMetaKey *key)
{
	if (shared == NULL || !ch_enable_shared_part_metadata || t->storage.backend >= 0 || part->listing != NULL ||
		strlen(part->name) >= CH_PARTMETA_NAME)
		return false;
	memset(key, 0, sizeof(*key));
	key->dbid = t->storage.locator.dbOid;
	key->spcid = t->storage.locator.spcOid;
	key->relnumber = t->storage.locator.relNumber;
	key->part_rows = part->rows;
	key->part_created = part->created;
	strcpy(key->part, part->name);
	return true;
}

static uint32
key_hash(const ChPartMetaKey *key)
{
	uint32		h = hash_bytes((const unsigned char *) key, sizeof(*key));

	return h == 0 ? 1 : h;
}

/* The slot of a key, or of the empty one where its probe ends; under the lock */
static ChPartMetaSlot *
probe(const ChPartMetaKey *key, uint32 h)
{
	uint32		mask = shared->nslots - 1;

	for (uint32 i = h & mask;; i = (i + 1) & mask)
	{
		ChPartMetaSlot *s = &shared->slots[i];

		if (s->hash == 0 || (s->hash == h && memcmp(&s->key, key, sizeof(*key)) == 0))
			return s;
	}
}

/* A part's blob, copied into the current context, where an entry holds it; NULL where none does */
static char *
find(const ChPartMetaKey *key, size_t *len)
{
	uint32		h = key_hash(key);
	ChPartMetaSlot *s;
	char	   *blob = NULL;

	LWLockAcquire(shared->lock, LW_SHARED);
	s = probe(key, h);
	if (s->hash != 0)
	{
		blob = MemoryContextAllocExtended(CurrentMemoryContext, Max(s->len, 1), MCXT_ALLOC_NO_OOM);
		if (blob != NULL)
		{
			memcpy(blob, pool() + s->off, s->len);
			*len = s->len;
		}
	}
	LWLockRelease(shared->lock);
	if (blob != NULL)
		found_count++;
	return blob;
}

static void
keep(const ChPartMetaKey *key, const void *data, size_t len, const ChPartMetaFigure *figures, int nfigures)
{
	uint32		h = key_hash(key);
	ChPartMetaSlot *s;
	Size		need = MAXALIGN(len) + sizeof(ChPartMetaFigure) * nfigures;

	if (need > shared->pool_size || len > PG_UINT32_MAX)
		return;
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);
	/* full, the table or the pool: every entry goes, those of parts merged away or dropped among them */
	if (shared->used + 1 > (uint32) ch_part_metadata_entries || shared->pool_used + need > shared->pool_size)
		empty_all();
	s = probe(key, h);
	if (s->hash == 0)
	{
		shared->used++;
		s->hash = h;
		s->key = *key;
		s->off = shared->pool_used;
		s->len = (uint32) len;
		s->nfigures = nfigures;
		memcpy(pool() + s->off, data, len);
		if (nfigures > 0)
			memcpy(pool() + s->off + MAXALIGN(len), figures, sizeof(ChPartMetaFigure) * nfigures);
		shared->pool_used += need;
	}
	LWLockRelease(shared->lock);
	stored_count++;
}

/*
 * A part of the pages store attached in this process for the first time
 * (ch_pages_attach()): its metadata handed to the library where an entry
 * holds it -- the part then made of them as it is first loaded -- else the
 * part loaded now and its metadata kept for the next process
 */
void
ch_part_metadata_prepare(ChTable *t, const ChPart *part)
{
	const ChEngine *eng = ch_engine();
	ChPartMetaKey key;
	char	   *blob;
	size_t		len = 0;

	if (eng->part_metadata_import == NULL || eng->part_metadata_export == NULL || !make_key(t, part, &key))
		return;
	blob = find(&key, &len);
	if (blob != NULL)
	{
		int			taken = 0;

		CH_CALL("loading a part",
				eng->part_metadata_import(t->handle, part->name, blob, len, &taken, CH_ERR));
		pfree(blob);
		if (taken)
			taken_count++;
		return;
	}
	{
		const void *data;

		CH_CALL("loading a part", eng->part_metadata_export(t->handle, part->name, &data, &len, CH_ERR));
		if (len > 0)
		{
			ChPartMetaFigure *figures = palloc_array(ChPartMetaFigure, Max(t->natts, 1));
			int			nfigures = 0;

			/* VC46: each column's exact figures, in the attributes' order */
			for (int i = 0; eng->part_column_defaults != NULL && i < t->natts; i++)
			{
				char		column[16];
				uint64_t	rows;
				uint64_t	defaults;
				int			exact;

				if (t->columns[i].kind == CH_COL_DROPPED)
					continue;
				snprintf(column, sizeof(column), "c%d", i + 1);
				CH_CALL("reading a part's figures",
						eng->part_column_defaults(t->handle, part->name, column, &rows, &defaults, &exact, CH_ERR));
				if (exact)
				{
					figures[nfigures].attno = i + 1;
					figures[nfigures].rows = rows;
					figures[nfigures].defaults = defaults;
					nfigures++;
				}
			}
			keep(&key, data, len, figures, nfigures);
			pfree(figures);
		}
	}
}

/*
 * VC46: a part's rows and the rows holding a column's default where the library
 * counted them exactly and a process has kept them with the part's metadata;
 * false where no process has, or they were not exact: no part loaded for them.
 */
bool
ch_part_defaults(const ChTable *t, const ChPart *part, AttrNumber attno, uint64 *rows, uint64 *defaults)
{
	ChPartMetaKey key;
	ChPartMetaSlot *s;
	bool		found = false;

	if (!make_key(t, part, &key))
		return false;
	LWLockAcquire(shared->lock, LW_SHARED);
	s = probe(&key, key_hash(&key));
	if (s->hash != 0)
	{
		const ChPartMetaFigure *f = (const ChPartMetaFigure *) (pool() + s->off + MAXALIGN(s->len));

		for (uint32 i = 0; i < s->nfigures && !found; i++)
			if (f[i].attno == attno)
			{
				*rows = f[i].rows;
				*defaults = f[i].defaults;
				found = true;
			}
	}
	LWLockRelease(shared->lock);
	return found;
}

/*
 * ch.part_metadata(): the entries kept and their bytes, and the session's
 * process's finds, keeps and parts made of them -- NULLs but the last three
 * where none is kept (not preloaded, or the settings 0)
 */
PG_FUNCTION_INFO_V1(ch_part_metadata_info);
Datum
ch_part_metadata_info(PG_FUNCTION_ARGS)
{
	TupleDesc	desc;
	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};

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
	values[4] = Int64GetDatum((int64) taken_count);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(BlessTupleDesc(desc), values, nulls)));
}
