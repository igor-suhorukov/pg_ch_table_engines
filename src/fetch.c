/*-------------------------------------------------------------------------
 *
 * fetch.c
 *	  Fetches of rows by their TIDs (ch_table_engines.md §3, VC16): the
 *	  columns each one reads, and their price for the planner.
 *
 *	  A fetch reads a granule of each column it decodes (scan.c): of every
 *	  column of the table, 8.8 ms a row of ClickBench's 105, where the query
 *	  reads one or two of them, 0.30 ms for one.  So a fetch into a slot this
 *	  module knows reads only the columns named for it, the slot's others
 *	  left NULL, which no one reads:
 *
 *	  - the slots of the index, index-only, bitmap and TID scans of a
 *	    statement's plan, found once the executor's tree is made
 *	    (ExecutorStart_hook): a scan reads the Vars of its relation in its
 *	    quals, and in its target list -- all of it where it projects, which
 *	    computes every entry; else, its row handed up as it is, the entries
 *	    the nodes above read: the positions of its output each node above
 *	    reads, worked out from the top of the tree down, a join's, an
 *	    aggregate's and a projection's through every expression they
 *	    compute, a sort's and the like -- nodes that pass their input's rows
 *	    on -- through the entries read above them and the columns they name
 *	    -- and an index-only scan's table slot no column, its rows'
 *	    values coming from the index.  Every column where the relation is
 *	    one the statement modifies or locks -- its rows are fetched again
 *	    whole, which the last fetch of every column answers (scan.c) -- or a
 *	    whole row is read, or the node above is one whose reading of its
 *	    input this module does not know: a custom scan, a foreign scan, a
 *	    grouping by sets;
 *	  - a slot a batch source's caller names (vexec_source.h's
 *	    fetch_columns(), minor 4): vexec's late columns.
 *
 *	  Any other slot -- ModifyTable's, a trigger's, EvalPlanQual's -- is
 *	  filled with every column.  A slot's registration goes when the memory
 *	  context it was made in is reset, with the slot.
 *
 *	  The price.  PostgreSQL prices a fetch as a random page, where it
 *	  decodes a granule of each column it reads: so priced, an index on each
 *	  of TPC-H's keys made eight of its queries 3.6 to 2,203 times as slow,
 *	  their plans' costs within 5-25% of the hash joins'.  Each index's
 *	  amcostestimate is wrapped where the relation's RelOptInfo is made
 *	  (build_simple_rel_hook, after get_relation_info() set it; PostgreSQL
 *	  19 has no get_relation_info_hook), and adds the fetches' price to the
 *	  index's: ch.fetch_cost a row -- finding it, its row id's granule read
 *	  -- and ch.fetch_value_cost a value of a granule of each column it
 *	  decodes, 55 and 0.0025, fitted to four fetches of 0.19 to 8.8 ms
 *	  (§3, "VC4, VC6 and VC16, researched").  An index-only scan decodes no
 *	  column; where the index's first column is the table's first key
 *	  column, the rows it finds lie together, a granule holding
 *	  index_granularity of them, which a fetch's cache decodes once.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/amapi.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/extendplan.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/prep.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/varlena.h"

#include "chte.h"
#include "scan.h"

/* ---- the slots whose columns are known ---- */

typedef struct FetchSlot
{
	TupleTableSlot *slot;		/* the key */
	uint64		serial;			/* its registration's */
	Oid			relid;
	TupleDesc	desc;			/* the slot's, when it was registered */
	int			ncols;			/* -1: every column */
	AttrNumber *attnums;		/* 1-based, ascending, in the slot's context */
	MemoryContext cxt;			/* the slot's */
	ChFetchCache *cache;		/* its fetches', made at the first one */
} FetchSlot;

/* A registration's end, when the slot's context is reset */
typedef struct FetchForget
{
	MemoryContextCallback cb;
	TupleTableSlot *slot;
	uint64		serial;
} FetchForget;

static HTAB *fetch_slots = NULL;
static uint64 fetch_serial = 0;

static void
forget_slot(void *arg)
{
	FetchForget *f = arg;
	FetchSlot  *e;

	if (fetch_slots == NULL)
		return;
	e = hash_search(fetch_slots, &f->slot, HASH_FIND, NULL);
	if (e != NULL && e->serial == f->serial)
		(void) hash_search(fetch_slots, &f->slot, HASH_REMOVE, NULL);
}

/*
 * The columns attnums[0 .. n) (1-based) of the relation are those fetches by
 * TID into the slot read, or every column where n < 0; until the context the
 * slot was made in is reset.
 */
void
ch_fetch_set_columns(Relation rel, TupleTableSlot *slot, int n, const AttrNumber *attnums)
{
	MemoryContext cxt = GetMemoryChunkContext(slot);
	FetchSlot  *e;
	FetchForget *f;
	AttrNumber *copy = NULL;
	bool		found;

	if (fetch_slots == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(TupleTableSlot *);
		ctl.entrysize = sizeof(FetchSlot);
		ctl.hcxt = TopMemoryContext;
		fetch_slots = hash_create("ch fetch slots", 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	if (n >= 0)
	{
		copy = MemoryContextAlloc(cxt, sizeof(AttrNumber) * Max(n, 1));
		for (int i = 0; i < n; i++)
			copy[i] = attnums[i];
	}
	f = MemoryContextAllocZero(cxt, sizeof(FetchForget));
	e = hash_search(fetch_slots, &slot, HASH_ENTER, &found);
	e->serial = ++fetch_serial;
	e->relid = RelationGetRelid(rel);
	e->desc = slot->tts_tupleDescriptor;
	e->ncols = n;
	e->attnums = copy;
	e->cxt = cxt;
	e->cache = NULL;
	f->slot = slot;
	f->serial = e->serial;
	f->cb.func = forget_slot;
	f->cb.arg = f;
	MemoryContextRegisterResetCallback(cxt, &f->cb);
}

/* The slot's registration for the relation, or NULL */
static FetchSlot *
slot_entry(Relation rel, TupleTableSlot *slot)
{
	FetchSlot  *e;

	if (fetch_slots == NULL)
		return NULL;
	e = hash_search(fetch_slots, &slot, HASH_FIND, NULL);
	if (e == NULL || e->relid != RelationGetRelid(rel) || e->desc != slot->tts_tupleDescriptor)
		return NULL;
	return e;
}

/* The columns a fetch into the slot reads, where they are not every column */
bool
ch_fetch_slot_columns(Relation rel, TupleTableSlot *slot, int *n, const AttrNumber **attnums)
{
	FetchSlot  *e = slot_entry(rel, slot);

	if (e == NULL || e->ncols < 0)
		return false;
	*n = e->ncols;
	*attnums = e->attnums;
	return true;
}

/* The cache of a registered slot's fetches, made at its first; NULL for another slot */
ChFetchCache *
ch_fetch_cache_for(Relation rel, TupleTableSlot *slot)
{
	FetchSlot  *e = slot_entry(rel, slot);

	if (e == NULL)
		return NULL;
	if (e->cache == NULL)
		e->cache = ch_fetch_cache_create(e->cxt, rel, e->ncols, e->attnums);
	return e->cache;
}

/* ---- the columns of a plan's scans ---- */

/*
 * The positions of a node's output that the nodes above read: every one, or
 * those of pos, from 1
 */
typedef struct Need
{
	bool		all;
	Bitmapset  *pos;
} Need;

/* The attributes of one varno an expression reads */
typedef struct VarsRead
{
	int			varno;
	Bitmapset  *attrs;
	bool		all;			/* a whole row, or a column not by number */
} VarsRead;

static bool
vars_walker(Node *node, VarsRead *v)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varno == v->varno && var->varlevelsup == 0)
		{
			if (var->varattno > 0)
				v->attrs = bms_add_member(v->attrs, var->varattno);
			else if (var->varattno == InvalidAttrNumber ||
					 v->varno == OUTER_VAR || v->varno == INNER_VAR)
				v->all = true;
			/* ctid and tableoid come from the slot itself */
		}
		return false;
	}
	return expression_tree_walker(node, vars_walker, v);
}

static void
read_expr(VarsRead *v, void *expr)
{
	(void) vars_walker((Node *) expr, v);
}

/* The entries of a target list at the positions read above */
static void
read_tlist(VarsRead *v, List *tlist, const Need *need)
{
	ListCell   *lc;

	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (need->all || bms_is_member(tle->resno, need->pos))
			read_expr(v, tle->expr);
	}
}

/*
 * Columns a node names by their positions -- a sort's keys, a grouping's --
 * as positions of its input, and as its own target list's entries there,
 * which a node that passes its input's rows on reads them through
 */
static void
read_cols(VarsRead *v, List *tlist, int n, const AttrNumber *cols)
{
	for (int i = 0; i < n; i++)
	{
		v->attrs = bms_add_member(v->attrs, cols[i]);
		if (cols[i] >= 1 && cols[i] <= list_length(tlist))
			read_expr(v, list_nth_node(TargetEntry, tlist, cols[i] - 1)->expr);
	}
}

/*
 * The entries of a node's target list it computes: every one where it
 * projects -- a projection computes all of them, an aggregation all its
 * aggregates, whichever the nodes above read, and an entry over a column
 * left NULL could raise where its value would not -- and where it passes
 * its input's rows on, those read above, which it does not compute
 */
static const Need *
computed(PlanState *ps, const Need *need)
{
	static const Need all = {true, NULL};

	if (ps->ps_ProjInfo != NULL || IsA(ps->plan, Agg) || IsA(ps->plan, Group) ||
		IsA(ps->plan, WindowAgg) || IsA(ps->plan, ProjectSet) || IsA(ps->plan, Result))
		return &all;
	return need;
}

/*
 * The positions of a node's outer and inner inputs it reads, given the
 * positions of its output read above; false where it is a node whose
 * reading of its inputs is not known here.
 */
static bool
input_needs(PlanState *ps, const Need *need, Need *outer, Need *inner)
{
	Plan	   *plan = ps->plan;
	VarsRead	o = {.varno = OUTER_VAR};
	VarsRead	i = {.varno = INNER_VAR};
	ListCell   *lc;

	need = computed(ps, need);
	read_tlist(&o, plan->targetlist, need);
	read_tlist(&i, plan->targetlist, need);
	read_expr(&o, plan->qual);
	read_expr(&i, plan->qual);
	switch (nodeTag(plan))
	{
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			if (IsA(plan, NestLoop))
				foreach(lc, ((NestLoop *) plan)->nestParams)
					read_expr(&o, lfirst_node(NestLoopParam, lc)->paramval);
			read_expr(&o, ((Join *) plan)->joinqual);
			read_expr(&i, ((Join *) plan)->joinqual);
			if (IsA(plan, MergeJoin))
			{
				read_expr(&o, ((MergeJoin *) plan)->mergeclauses);
				read_expr(&i, ((MergeJoin *) plan)->mergeclauses);
			}
			else if (IsA(plan, HashJoin))
			{
				read_expr(&o, ((HashJoin *) plan)->hashclauses);
				read_expr(&i, ((HashJoin *) plan)->hashclauses);
				read_expr(&o, ((HashJoin *) plan)->hashkeys);
			}
			break;
		case T_Hash:
			read_expr(&o, ((Hash *) plan)->hashkeys);
			break;
		case T_Sort:
		case T_IncrementalSort:
			read_cols(&o, plan->targetlist, ((Sort *) plan)->numCols, ((Sort *) plan)->sortColIdx);
			break;
		case T_GatherMerge:
			read_cols(&o, plan->targetlist, ((GatherMerge *) plan)->numCols,
					  ((GatherMerge *) plan)->sortColIdx);
			break;
		case T_Material:
		case T_Gather:
		case T_Result:
		case T_ProjectSet:
			if (IsA(plan, Result))
				read_expr(&o, ((Result *) plan)->resconstantqual);
			break;
		case T_Memoize:
			read_expr(&o, ((Memoize *) plan)->param_exprs);
			break;
		case T_Limit:
			read_expr(&o, ((Limit *) plan)->limitOffset);
			read_expr(&o, ((Limit *) plan)->limitCount);
			read_cols(&o, plan->targetlist, ((Limit *) plan)->uniqNumCols, ((Limit *) plan)->uniqColIdx);
			break;
		case T_Unique:
			read_cols(&o, plan->targetlist, ((Unique *) plan)->numCols, ((Unique *) plan)->uniqColIdx);
			break;
		case T_Group:
			read_cols(&o, plan->targetlist, ((Group *) plan)->numCols, ((Group *) plan)->grpColIdx);
			break;
		case T_Agg:
			if (((Agg *) plan)->groupingSets != NIL || ((Agg *) plan)->chain != NIL)
				return false;
			read_cols(&o, plan->targetlist, ((Agg *) plan)->numCols, ((Agg *) plan)->grpColIdx);
			break;
		case T_WindowAgg:
			{
				WindowAgg  *w = (WindowAgg *) plan;

				read_cols(&o, plan->targetlist, w->partNumCols, w->partColIdx);
				read_cols(&o, plan->targetlist, w->ordNumCols, w->ordColIdx);
				read_expr(&o, w->runCondition);
				read_expr(&o, w->runConditionOrig);
				read_expr(&o, w->startOffset);
				read_expr(&o, w->endOffset);
			}
			break;
		default:
			return false;
	}
	outer->all = o.all;
	outer->pos = o.attrs;
	inner->all = i.all;
	inner->pos = i.attrs;
	return true;
}

typedef struct WalkState
{
	PlannedStmt *stmt;
} WalkState;

/* Whether the statement modifies or locks the range table entry's rows */
static bool
rti_written(WalkState *ws, Index rti)
{
	ListCell   *lc;

	if (bms_is_member((int) rti, ws->stmt->resultRelationRelids))
		return true;
	foreach(lc, ws->stmt->rowMarks)
		if (lfirst_node(PlanRowMark, lc)->rti == rti)
			return true;
	return false;
}

/* A scan of a table of the engines that fetches by TID: its slot's columns */
static void
scan_columns(PlanState *ps, const Need *need, WalkState *ws)
{
	Plan	   *plan = ps->plan;
	Scan	   *scan = (Scan *) plan;
	Relation	rel = ((ScanState *) ps)->ss_currentRelation;
	TupleTableSlot *slot = ((ScanState *) ps)->ss_ScanTupleSlot;
	VarsRead	v = {.varno = (int) scan->scanrelid};
	AttrNumber *attnums;
	int			n = 0;
	int			x = -1;

	if (rel == NULL || !ch_is_mvcc_rel(rel))
		return;
	if (IsA(ps, IndexOnlyScanState))
	{
		/* its rows' values are the index's: the table's row only says it is there */
		ch_fetch_set_columns(rel, ((IndexOnlyScanState *) ps)->ioss_TableSlot, 0, NULL);
		return;
	}
	if (slot == NULL)
		return;
	if (rti_written(ws, scan->scanrelid))
	{
		ch_fetch_set_columns(rel, slot, -1, NULL);
		return;
	}
	/* a scan that projects computes its whole target list; one that does not hands its row up */
	read_tlist(&v, plan->targetlist, computed(ps, need));
	read_expr(&v, plan->qual);
	switch (nodeTag(plan))
	{
		case T_IndexScan:
			read_expr(&v, ((IndexScan *) plan)->indexqualorig);
			read_expr(&v, ((IndexScan *) plan)->indexorderbyorig);
			break;
		case T_BitmapHeapScan:
			read_expr(&v, ((BitmapHeapScan *) plan)->bitmapqualorig);
			break;
		case T_TidScan:
			read_expr(&v, ((TidScan *) plan)->tidquals);
			break;
		default:
			break;
	}
	if (v.all)
	{
		ch_fetch_set_columns(rel, slot, -1, NULL);
		return;
	}
	attnums = palloc(sizeof(AttrNumber) * Max(bms_num_members(v.attrs), 1));
	while ((x = bms_next_member(v.attrs, x)) >= 0)
		if (x <= RelationGetNumberOfAttributes(rel))
			attnums[n++] = (AttrNumber) x;
	ch_fetch_set_columns(rel, slot, n, attnums);
	pfree(attnums);
}

typedef struct ChildWalk
{
	PlanState  *parent;
	Need		self;			/* the parent's output, read above */
	Need		outer;
	Need		inner;
	Need		sub;			/* a SubqueryScan's subplan */
	bool		known;
	WalkState  *ws;
} ChildWalk;

static void walk_node(PlanState *ps, const Need *need, WalkState *ws);

static bool
walk_child(PlanState *child, void *arg)
{
	ChildWalk  *c = arg;
	Need		all = {true, NULL};
	const Need *need = &all;

	if (!c->known)
		need = &all;
	else if (child == outerPlanState(c->parent))
		need = &c->outer;
	else if (child == innerPlanState(c->parent))
		need = &c->inner;
	else if (IsA(c->parent, AppendState) || IsA(c->parent, MergeAppendState))
		need = &c->self;		/* their children's rows are theirs, column for column */
	else if (IsA(c->parent, SubqueryScanState) && child == ((SubqueryScanState *) c->parent)->subplan)
		need = &c->sub;
	walk_node(child, need, c->ws);
	return false;
}

static void
walk_node(PlanState *ps, const Need *need, WalkState *ws)
{
	ChildWalk	c;

	check_stack_depth();
	memset(&c, 0, sizeof(c));
	c.parent = ps;
	c.self = *need;
	c.ws = ws;
	switch (nodeTag(ps))
	{
		case T_IndexScanState:
		case T_IndexOnlyScanState:
		case T_BitmapHeapScanState:
		case T_TidScanState:
			scan_columns(ps, need, ws);
			c.known = true;		/* their children are a bitmap's index scans */
			c.outer.all = c.inner.all = true;
			break;
		case T_AppendState:
		case T_MergeAppendState:
			c.known = true;
			if (IsA(ps, MergeAppendState))
			{
				MergeAppend *ma = (MergeAppend *) ps->plan;

				c.self.pos = bms_copy(need->pos);
				for (int i = 0; i < ma->numCols; i++)
					c.self.pos = bms_add_member(c.self.pos, ma->sortColIdx[i]);
			}
			break;
		case T_SubqueryScanState:
			{
				VarsRead	v = {.varno = (int) ((Scan *) ps->plan)->scanrelid};

				read_tlist(&v, ps->plan->targetlist, computed(ps, need));
				read_expr(&v, ps->plan->qual);
				c.known = true;
				c.sub.all = v.all;
				c.sub.pos = v.attrs;
			}
			break;
		default:
			c.known = input_needs(ps, need, &c.outer, &c.inner);
			break;
	}
	(void) planstate_tree_walker(ps, walk_child, &c);
}

/* Whether a plan holds a scan of a table of the engines that fetches by TID */
static bool
has_fetching_scan(PlanState *ps, void *arg)
{
	if (ps == NULL)
		return false;
	switch (nodeTag(ps))
	{
		case T_IndexScanState:
		case T_IndexOnlyScanState:
		case T_BitmapHeapScanState:
		case T_TidScanState:
			if (((ScanState *) ps)->ss_currentRelation != NULL &&
				ch_is_mvcc_rel(((ScanState *) ps)->ss_currentRelation))
				return true;
			break;
		default:
			break;
	}
	return planstate_tree_walker(ps, has_fetching_scan, arg);
}

static ExecutorStart_hook_type prev_ExecutorStart = NULL;

static void
ch_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
	if (ch_enable_fetch_columns && queryDesc->planstate != NULL &&
		has_fetching_scan(queryDesc->planstate, NULL))
	{
		Need		all = {true, NULL};
		WalkState	ws = {.stmt = queryDesc->plannedstmt};
		MemoryContext old = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);

		walk_node(queryDesc->planstate, &all, &ws);
		MemoryContextSwitchTo(old);
	}
}

/* ---- the price of a fetch ---- */

/* A fetch of ncols columns of the relation, a row's, in the planner's units */
double
ch_fetch_price(Relation rel, int ncols)
{
	ChOptions	opts;

	/*
	 * an external table's row is read from its file's page of each column: a
	 * Parquet page, a record batch -- some 128K values, as their writers make
	 * them (§2.17), where a granule is index_granularity's
	 */
	if (ch_is_external_rel(rel, NULL))
		return ch_fetch_cost + 131072.0 * ncols * ch_fetch_value_cost;
	ch_relation_options(rel, &opts);
	return ch_fetch_cost + (double) opts.index_granularity * ncols * ch_fetch_value_cost;
}

/* An index of an engine's table, and its access method's own estimate */
typedef struct IndexPrice
{
	IndexOptInfo *index;
	void		(*amcostestimate) (struct PlannerInfo *, struct IndexPath *, double, Cost *, Cost *,
								   Selectivity *, double *, double *);
} IndexPrice;

/* What the wrapper needs of a relation of an engine's table */
typedef struct RelPrice
{
	int			granularity;	/* index_granularity */
	AttrNumber	first_key;		/* the first order_by column, or 0 */
	int			live;			/* its columns */
	List	   *indexes;		/* IndexPrice */
} RelPrice;

static int	planner_id = -1;
static build_simple_rel_hook_type prev_build_simple_rel = NULL;

/* The columns of the relation a path's fetches decode: every one, or a count */
static int
fetched_columns(PlannerInfo *root, RelOptInfo *rel, IndexPath *path, const RelPrice *rp)
{
	Bitmapset  *attrs = NULL;
	ListCell   *lc;
	int			n = 0;
	int			x = -1;

	if (path->path.pathtype == T_IndexOnlyScan)
		return 0;
	if (root->parse->resultRelation == (int) rel->relid ||
		get_plan_rowmark(root->rowMarks, rel->relid) != NULL)
		return rp->live;
	pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &attrs);
	foreach(lc, rel->baserestrictinfo)
		pull_varattnos((Node *) lfirst_node(RestrictInfo, lc)->clause, rel->relid, &attrs);
	foreach(lc, path->indexclauses)
		pull_varattnos((Node *) lfirst_node(IndexClause, lc)->rinfo->clause, rel->relid, &attrs);
	pull_varattnos((Node *) path->indexorderbys, rel->relid, &attrs);
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs))
		return rp->live;
	while ((x = bms_next_member(attrs, x)) >= 0)
		if (x + FirstLowInvalidHeapAttributeNumber > 0)
			n++;
	return Min(n, rp->live);
}

/*
 * An index's estimate, its access method's, and its fetches' price beside
 * it: ch.fetch_cost a row, and a granule of each column decoded -- a granule
 * a row, or one for each index_granularity rows where the index's first
 * column is the table's first key column, its rows of a range together.
 */
static void
ch_amcostestimate(PlannerInfo *root, IndexPath *path, double loop_count, Cost *startup, Cost *total,
				  Selectivity *selectivity, double *correlation, double *pages)
{
	IndexOptInfo *index = path->indexinfo;
	RelOptInfo *rel = index->rel;
	RelPrice   *rp = GetRelOptInfoExtensionState(rel, planner_id);
	IndexPrice *ip = NULL;
	ListCell   *lc;
	double		tuples;
	double		granules;
	int			ncols;

	if (rp != NULL)
		foreach(lc, rp->indexes)
			if (((IndexPrice *) lfirst(lc))->index == index)
				ip = lfirst(lc);
	if (ip != NULL)
		ip->amcostestimate(root, path, loop_count, startup, total, selectivity, correlation, pages);
	else
	{
		/* a copy of the index's entry: its access method's own estimate alone */
		GetIndexAmRoutineByAmId(index->relam, false)->amcostestimate(root, path, loop_count, startup, total,
																	 selectivity, correlation, pages);
		return;
	}
	if (ch_fetch_cost <= 0 && ch_fetch_value_cost <= 0)
		return;

	tuples = clamp_row_est(*selectivity * rel->tuples);
	ncols = fetched_columns(root, rel, path, rp);
	granules = tuples;
	if (rp->first_key != 0 && index->ncolumns > 0 && index->indexkeys[0] == rp->first_key)
		granules = ceil(tuples / Max(rp->granularity, 1));
	*total += tuples * ch_fetch_cost + granules * (double) rp->granularity * ncols * ch_fetch_value_cost;
}

/*
 * A relation of an engine's table being planned: each of its indexes'
 * estimates wrapped, which get_relation_info() has just set.
 */
static void
ch_build_simple_rel(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relation	relation;

	if (prev_build_simple_rel)
		prev_build_simple_rel(root, rel, rte);
	if (rte->rtekind != RTE_RELATION || rel->indexlist == NIL)
		return;
	relation = table_open(rte->relid, NoLock);
	if (ch_is_mvcc_rel(relation))
	{
		RelPrice   *rp = palloc0(sizeof(RelPrice));
		TupleDesc	desc = RelationGetDescr(relation);
		ChOptions	opts;
		ListCell   *lc;

		ch_relation_options(relation, &opts);
		/* a table of its own files' row is read from its part's page of each column (§2.18) */
		rp->granularity = ch_is_own_rel(relation) ? 131072 : opts.index_granularity;
		if (opts.order_by != NULL)
		{
			char	   *copy = pstrdup(opts.order_by);
			List	   *names;

			if (SplitIdentifierString(copy, ',', &names) && names != NIL)
				rp->first_key = get_attnum(RelationGetRelid(relation), linitial(names));
		}
		for (int i = 0; i < desc->natts; i++)
			if (!TupleDescAttr(desc, i)->attisdropped)
				rp->live++;
		foreach(lc, rel->indexlist)
		{
			IndexOptInfo *index = lfirst(lc);
			IndexPrice *ip = palloc(sizeof(IndexPrice));

			ip->index = index;
			ip->amcostestimate = index->amcostestimate;
			rp->indexes = lappend(rp->indexes, ip);
			index->amcostestimate = ch_amcostestimate;
		}
		SetRelOptInfoExtensionState(rel, planner_id, rp);
	}
	table_close(relation, NoLock);
}

/* ---- ch.fetch_stats() ---- */

PG_FUNCTION_INFO_V1(ch_fetch_stats);
Datum
ch_fetch_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	desc;
	Datum		values[6];
	bool		nulls[6] = {false};

	if (get_call_result_type(fcinfo, NULL, &desc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	values[0] = Int64GetDatum(ch_fetch_counts.fetches);
	values[1] = Int64GetDatum(ch_fetch_counts.values);
	values[2] = Int64GetDatum(ch_fetch_counts.whole);
	values[3] = Int64GetDatum(ch_fetch_counts.kept);
	values[4] = Int64GetDatum(ch_fetch_counts.located);
	values[5] = Int64GetDatum(ch_fetch_counts.pointed);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(BlessTupleDesc(desc), values, nulls)));
}

void
ch_fetch_init(void)
{
	planner_id = GetPlannerExtensionId("ch_table_engines");
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = ch_ExecutorStart;
	prev_build_simple_rel = build_simple_rel_hook;
	build_simple_rel_hook = ch_build_simple_rel;
}
