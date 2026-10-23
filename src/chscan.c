/*-------------------------------------------------------------------------
 *
 * chscan.c
 *	  ChScan (ch_table_engines.md §2.8, "Reads" 5): the extension's
 *	  CustomScan over a table of an engine, offered at set_rel_pathlist_hook
 *	  beside the sequential scan, which reads every column.  ChScan reads
 *	  the columns its quals and target name, and gives its quals to
 *	  ClickHouse's analysis as a filter (filter.c), so that the granules the
 *	  key and the parts' statistics rule out are not read; the executor
 *	  evaluates every qual on what is read, as a SeqScan's are.
 *
 *	  It is costed as cost_seqscan() costs a sequential scan
 *	  (PG19:src/backend/optimizer/path/costsize.c:270-338), with the needed
 *	  columns' compressed bytes in the granules the quals keep in place of
 *	  the relation's pages, and their rows in place of its tuples.
 *
 *	  Parallel-aware: a partial path for a Gather, its participants sharing
 *	  the table's units through the access method's parallel descriptor in
 *	  the node's chunk of the DSM, as nodeSeqscan.c shares a SeqScan's, and
 *	  adding what each read to counters beside it, for EXPLAIN ANALYZE.  Its
 *	  workers are those compute_parallel_worker() would give a SeqScan
 *	  (allpaths.c:866-878, 4963-5038) over a heap table of the columns read
 *	  holding the rows read: a narrow scan's work, little of which is its
 *	  compressed bytes, is mostly the executor's a row, as a SeqScan's is.
 *
 *	  A cursor's: WHERE CURRENT OF takes the TID of the row its scan is on
 *	  from the scan's slot, which must have one (execCurrent.c:211-246), so
 *	  a ChScan planned for a cursor -- a plan made with CURSOR_OPT_FAST_PLAN,
 *	  which DECLARE and PL/pgSQL's cursors give (gram.y:13020) -- reads the
 *	  row ids whatever its target names.
 *
 *	  A writer's: ModifyTable fetches an UPDATE's old row by its TID (and a
 *	  DELETE's, for RETURNING and row triggers), and a row lock its row; a
 *	  ChScan of such a relation reads every column, so that the fetch takes
 *	  the row from the scan's batch (scan.c) rather than from the parts.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_class.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "port/atomics.h"
#include "storage/itemid.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/spccache.h"

#include "chte.h"
#include "scan.h"

bool		ch_enable_chscan = true;

static set_rel_pathlist_hook_type prev_set_rel_pathlist = NULL;
static planner_hook_type prev_planner = NULL;

/* The cursor options of the plan being made, innermost first */
static int	planning_cursor_options = 0;

static void add_chscan_paths(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);

static Plan *chscan_plan(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
						 List *clauses, List *custom_plans);
static Node *chscan_create_state(CustomScan *cscan);

static const CustomPathMethods chscan_path_methods = {
	.CustomName = "ChScan",
	.PlanCustomPath = chscan_plan,
};

static const CustomScanMethods chscan_plan_methods = {
	.CustomName = "ChScan",
	.CreateCustomScanState = chscan_create_state,
};

/* What the participants of a parallel scan read, added up for EXPLAIN ANALYZE */
typedef struct ChScanShared
{
	pg_atomic_uint64 parts_pruned;
	pg_atomic_uint64 units_passed;
	pg_atomic_uint64 marks;
	pg_atomic_uint64 marks_read;
} ChScanShared;

typedef struct ChScanState
{
	CustomScanState css;
	Relation	rel;
	TableScanDesc scan;
	int			natts;			/* the attributes read */
	AttrNumber *attnums;
	bool		need_tid;
	List	   *pruning;		/* the quals given as a filter */
	ChScanShared *shared;		/* a parallel scan's, in the DSM */
	ParallelTableScanDesc pscan;
	ChScanCounts counts;		/* this participant's, kept at its end */
	ChScanCounts others;		/* the workers', from the DSM at shutdown */
	bool		counted;
} ChScanState;

/* ---- planning ---- */

/* The share of a partial path's work each participant does, as get_parallel_divisor() */
static double
parallel_divisor(Path *path)
{
	double		divisor = path->parallel_workers;

	if (parallel_leader_participation)
	{
		double		leader = 1.0 - (0.3 * path->parallel_workers);

		if (leader > 0)
			divisor += leader;
	}
	return divisor;
}

/* The attributes a relation's scan needs, its quals' and its target's; false for a system one it cannot give */
static bool
needed_attrs(RelOptInfo *rel, List **attnums, bool *need_tid, bool *whole_row)
{
	Bitmapset  *attrs = NULL;
	ListCell   *lc;
	int			i = -1;

	pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &attrs);
	foreach(lc, rel->baserestrictinfo)
		pull_varattnos((Node *) ((RestrictInfo *) lfirst(lc))->clause, rel->relid, &attrs);
	*attnums = NIL;
	*need_tid = false;
	*whole_row = false;
	while ((i = bms_next_member(attrs, i)) >= 0)
	{
		AttrNumber	attno = i + FirstLowInvalidHeapAttributeNumber;

		if (attno == 0)
			*whole_row = true;
		else if (attno == SelfItemPointerAttributeNumber)
			*need_tid = true;
		else if (attno == TableOidAttributeNumber)
			continue;
		else if (attno < 0)
			return false;		/* xmin and the rest are heap's */
		else
			*attnums = lappend_int(*attnums, attno);
	}
	return true;
}

/*
 * Whether the executor fetches the relation's rows by TID after the scan
 * returns them: the result relation of an UPDATE or a MERGE, which builds the
 * new row from the old (nodeModifyTable.c); of a DELETE with RETURNING or row
 * triggers, which take the deleted row; a relation of FOR UPDATE and the like,
 * whose locks return the locked row.
 */
static bool
fetches_old_rows(PlannerInfo *root, Index rti, Relation relation)
{
	Query	   *parse = root->parse;

	if (get_parse_rowmark(parse, rti) != NULL)
		return true;
	if (!bms_is_member(rti, root->all_result_relids) && parse->resultRelation != (int) rti)
		return false;
	switch (parse->commandType)
	{
		case CMD_UPDATE:
		case CMD_MERGE:
			return true;
		case CMD_DELETE:
			return parse->returningList != NIL ||
				(relation->trigdesc && (relation->trigdesc->trig_delete_before_row ||
										relation->trigdesc->trig_delete_after_row ||
										relation->trigdesc->trig_delete_old_table));
		default:
			return false;
	}
}

/* The quals' cost a row, the pushed-down join clauses' too, as costsize.c's get_restriction_qual_cost() */
static void
restriction_qual_cost(PlannerInfo *root, RelOptInfo *rel, ParamPathInfo *param_info, QualCost *cost)
{
	if (param_info)
	{
		cost_qual_eval(cost, param_info->ppi_clauses, root);
		cost->startup += rel->baserestrictcost.startup;
		cost->per_tuple += rel->baserestrictcost.per_tuple;
	}
	else
		*cost = rel->baserestrictcost;
}

static PlannedStmt *
ch_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams,
		   ExplainState *es)
{
	int			save = planning_cursor_options;
	PlannedStmt *result;

	planning_cursor_options = cursorOptions;
	PG_TRY();
	{
		result = prev_planner ? prev_planner(parse, query_string, cursorOptions, boundParams, es) :
			standard_planner(parse, query_string, cursorOptions, boundParams, es);
	}
	PG_FINALLY();
	{
		planning_cursor_options = save;
	}
	PG_END_TRY();
	return result;
}

static CustomPath *
make_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer, int workers, double rows_read,
		  double bytes, bool rowid, bool whole)
{
	CustomPath *cp = makeNode(CustomPath);
	Path	   *path = &cp->path;
	ParamPathInfo *param_info = get_baserel_parampathinfo(root, rel, required_outer);
	QualCost	qual_cost;
	double		spc_seq_page_cost;
	Cost		startup = 0;
	Cost		cpu;
	Cost		disk;
	uint64		enable_mask = PGS_SEQSCAN;

	path->pathtype = T_CustomScan;
	path->parent = rel;
	path->pathtarget = rel->reltarget;
	path->param_info = param_info;
	path->parallel_aware = workers > 0;
	path->parallel_safe = rel->consider_parallel;
	path->parallel_workers = workers;
	path->pathkeys = NIL;
	path->rows = param_info ? param_info->ppi_rows : rel->rows;

	get_tablespace_page_costs(rel->reltablespace, NULL, &spc_seq_page_cost);
	disk = spc_seq_page_cost * ceil(bytes / BLCKSZ);
	restriction_qual_cost(root, rel, param_info, &qual_cost);
	startup += qual_cost.startup + path->pathtarget->cost.startup;
	cpu = (cpu_tuple_cost + qual_cost.per_tuple) * rows_read + path->pathtarget->cost.per_tuple * path->rows;
	if (workers > 0)
	{
		double		divisor = parallel_divisor(path);

		cpu /= divisor;
		path->rows = clamp_row_est(path->rows / divisor);
	}
	else
		enable_mask |= PGS_CONSIDER_NONPARTIAL;
	path->disabled_nodes = (rel->pgs_mask & enable_mask) == enable_mask ? 0 : 1;
	path->startup_cost = startup;
	path->total_cost = startup + cpu + disk;

	cp->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	cp->custom_private = list_make2_int(rowid ? 1 : 0, whole ? 1 : 0);
	cp->methods = &chscan_path_methods;
	return cp;
}

/*
 * Whether vexec's force mode has made its vector scan the relation's scan
 * (pg_accel:modules/vexec/plan/paths.c, consider_scan()): its hook ran
 * first, ours being the one it calls after.  vexec is not linked, and its
 * setting read by name.
 */
static bool
vexec_forced(RelOptInfo *rel)
{
	const char *mode = GetConfigOption("vexec.mode", true, false);
	ListCell   *lc;

	if (mode == NULL || strcmp(mode, "force") != 0)
		return false;
	foreach(lc, rel->pathlist)
	{
		Path	   *path = lfirst(lc);

		if (IsA(path, CustomPath) && strcmp(((CustomPath *) path)->methods->CustomName, "VecScan") == 0)
			return true;
	}
	return false;
}

/*
 * ChScan's paths, added before the hooks of the modules loaded before this
 * one run -- as core's own paths are -- so that one which replaces a
 * relation's paths, as vexec's force mode does, replaces ChScan's too.
 */
static void
ch_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	add_chscan_paths(root, rel, rte);
	if (prev_set_rel_pathlist)
		prev_set_rel_pathlist(root, rel, rti, rte);
}

static void
add_chscan_paths(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relation	relation;
	List	   *attnums;
	bool		need_tid;
	bool		whole_row;
	AttrNumber *array;
	int			n = 0;
	ListCell   *lc;
	List	   *quals = NIL;
	double		rows_read;
	double		bytes;
	double		raw;
	bool		whole = false;

	if (!ch_enable_chscan || rte->rtekind != RTE_RELATION || rte->inh ||
		(rte->relkind != RELKIND_RELATION && rte->relkind != RELKIND_MATVIEW) ||
		(rel->reloptkind != RELOPT_BASEREL && rel->reloptkind != RELOPT_OTHER_MEMBER_REL) ||
		IS_DUMMY_REL(rel) || rte->tablesample != NULL || vexec_forced(rel))
		return;
	/*
	 * WHERE CURRENT OF: the TID scan is the one path the executor can serve
	 * it by (tidpath.c, create_tidscan_paths()), the only one core adds
	 */
	foreach(lc, rel->baserestrictinfo)
		if (IsA(((RestrictInfo *) lfirst(lc))->clause, CurrentOfExpr))
			return;
	relation = table_open(rte->relid, NoLock);
	if (!ch_is_scan_rel(relation) || !needed_attrs(rel, &attnums, &need_tid, &whole_row))
	{
		table_close(relation, NoLock);
		return;
	}
	if (whole_row)
	{
		/* every attribute */
		list_free(attnums);
		attnums = NIL;
		for (int i = 0; i < RelationGetNumberOfAttributes(relation); i++)
			if (!TupleDescAttr(RelationGetDescr(relation), i)->attisdropped)
				attnums = lappend_int(attnums, i + 1);
	}
	array = palloc(sizeof(AttrNumber) * Max(Max(list_length(attnums), RelationGetNumberOfAttributes(relation)), 1));
	foreach(lc, attnums)
		array[n++] = (AttrNumber) lfirst_int(lc);
	/* the relation's own restrictions: a lateral one names another relation's Vars */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = lfirst(lc);

		if (bms_is_subset(rinfo->clause_relids, rel->relids))
			quals = lappend(quals, rinfo->clause);
	}
	/* a cursor's scan gives its rows' TIDs, for WHERE CURRENT OF */
	if (planning_cursor_options & CURSOR_OPT_FAST_PLAN)
		need_tid = true;
	/* a writer's: every column, for the fetch of its old rows by TID (above) */
	if (fetches_old_rows(root, rel->relid, relation))
	{
		whole = true;
		need_tid = true;
		list_free(attnums);
		attnums = NIL;
		n = 0;
		for (int i = 0; i < RelationGetNumberOfAttributes(relation); i++)
			if (!TupleDescAttr(RelationGetDescr(relation), i)->attisdropped)
				array[n++] = i + 1;
	}
	ch_estimate(relation, ActiveSnapshotSet() ? GetActiveSnapshot() : NULL, n, array, need_tid, quals,
				&rows_read, &bytes, &raw);
	table_close(relation, NoLock);

	add_path(rel, &make_path(root, rel, rel->lateral_relids, 0, rows_read, bytes, need_tid, whole)->path);
	if (rel->consider_parallel && rel->lateral_relids == NULL)
	{
		/* a heap tuple's header and line pointer a row, beside the columns' bytes */
		double		heap_bytes = raw + rows_read * (MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData));
		int			workers = compute_parallel_worker(rel, ceil(heap_bytes / BLCKSZ), -1,
													  max_parallel_workers_per_gather);

		if (workers > 0)
			add_partial_path(rel, &make_path(root, rel, NULL, workers, rows_read, bytes, need_tid, whole)->path);
	}
}

static Plan *
chscan_plan(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist, List *clauses,
			List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = extract_actual_clauses(clauses, false);
	cscan->scan.scanrelid = rel->relid;
	cscan->flags = best_path->flags;
	cscan->custom_private = best_path->custom_private;
	cscan->custom_scan_tlist = NIL;
	cscan->methods = &chscan_plan_methods;
	return &cscan->scan.plan;
}

/* ---- execution ---- */

static void chscan_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *chscan_exec(CustomScanState *node);
static void chscan_end(CustomScanState *node);
static void chscan_rescan(CustomScanState *node);
static Size chscan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt);
static void chscan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate);
static void chscan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate);
static void chscan_initialize_worker(CustomScanState *node, shm_toc *toc, void *coordinate);
static void chscan_shutdown(CustomScanState *node);
static void chscan_explain(CustomScanState *node, List *ancestors, ExplainState *es);

static const CustomExecMethods chscan_exec_methods = {
	.CustomName = "ChScan",
	.BeginCustomScan = chscan_begin,
	.ExecCustomScan = chscan_exec,
	.EndCustomScan = chscan_end,
	.ReScanCustomScan = chscan_rescan,
	.EstimateDSMCustomScan = chscan_estimate_dsm,
	.InitializeDSMCustomScan = chscan_initialize_dsm,
	.ReInitializeDSMCustomScan = chscan_reinitialize_dsm,
	.InitializeWorkerCustomScan = chscan_initialize_worker,
	.ShutdownCustomScan = chscan_shutdown,
	.ExplainCustomScan = chscan_explain,
};

static Node *
chscan_create_state(CustomScan *cscan)
{
	ChScanState *s = (ChScanState *) newNode(sizeof(ChScanState), T_CustomScanState);

	s->css.methods = &chscan_exec_methods;
	s->css.slotOps = &ch_slot_ops;
	return (Node *) s;
}

static void
chscan_begin(CustomScanState *node, EState *estate, int eflags)
{
	ChScanState *s = (ChScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Bitmapset  *attrs = NULL;
	int			i = -1;
	bool		whole_row = false;
	TupleDesc	desc;
	ChTable    *t;

	s->rel = node->ss.ss_currentRelation;
	desc = RelationGetDescr(s->rel);
	pull_varattnos((Node *) cscan->scan.plan.targetlist, cscan->scan.scanrelid, &attrs);
	pull_varattnos((Node *) cscan->scan.plan.qual, cscan->scan.scanrelid, &attrs);
	s->attnums = palloc(sizeof(AttrNumber) * Max(desc->natts, 1));
	while ((i = bms_next_member(attrs, i)) >= 0)
	{
		AttrNumber	attno = i + FirstLowInvalidHeapAttributeNumber;

		if (attno == 0)
			whole_row = true;
		else if (attno == SelfItemPointerAttributeNumber)
			s->need_tid = true;
		else if (attno > 0)
			s->attnums[s->natts++] = attno;
	}
	/* a writer's (above): every column */
	if (list_length(cscan->custom_private) > 1 && lsecond_int(cscan->custom_private))
		whole_row = true;
	if (whole_row)
	{
		s->natts = 0;
		for (int a = 0; a < desc->natts; a++)
			if (!TupleDescAttr(desc, a)->attisdropped)
				s->attnums[s->natts++] = a + 1;
	}
	/* a cursor's (above) */
	if (cscan->custom_private != NIL && linitial_int(cscan->custom_private))
		s->need_tid = true;
	/* pinned: the quals' translation looks operators up, which may accept an invalidation of the table */
	t = ch_table(s->rel);
	ch_table_pin(t);
	s->pruning = ch_filter_quals(t, cscan->scan.plan.qual);
	ch_table_unpin(t);
}

/* The table's scan, at the first row asked for: a share of a parallel one where the DSM holds one */
static void
begin_scan(ChScanState *s)
{
	EState	   *estate = s->css.ss.ps.state;
	uint32		flags = SO_NONE;

	if (ScanRelIsReadOnly(&s->css.ss))
		flags |= SO_HINT_REL_READ_ONLY;
	if (s->pscan)
		s->scan = table_beginscan_parallel(s->rel, s->pscan, flags);
	else
		s->scan = table_beginscan(s->rel, estate->es_snapshot, 0, NULL, flags);
	s->css.ss.ss_currentScanDesc = s->scan;
	ch_scan_set_columns(s->scan, s->natts, s->attnums, s->need_tid);
	if (s->pruning != NIL)
		(void) ch_scan_set_filter(s->scan, s->pruning);
}

static TupleTableSlot *
chscan_next(ScanState *ss)
{
	ChScanState *s = (ChScanState *) ss;
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;

	if (s->scan == NULL)
		begin_scan(s);
	if (table_scan_getnextslot(s->scan, ForwardScanDirection, slot))
		return slot;
	return NULL;
}

static bool
chscan_recheck(ScanState *ss, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
chscan_exec(CustomScanState *node)
{
	return ExecScan(&node->ss, chscan_next, chscan_recheck);
}

/* What this participant read, kept before its scan goes; a worker's added to the DSM's */
static void
keep_counts(ChScanState *s)
{
	const ChScanCounts *c;

	if (s->scan == NULL || s->counted)
		return;
	c = ch_scan_counts(s->scan);
	s->counts = *c;
	s->counted = true;
	if (s->shared && IsParallelWorker())
	{
		pg_atomic_fetch_add_u64(&s->shared->parts_pruned, c->parts_pruned);
		pg_atomic_fetch_add_u64(&s->shared->units_passed, c->units_passed);
		pg_atomic_fetch_add_u64(&s->shared->marks, c->marks);
		pg_atomic_fetch_add_u64(&s->shared->marks_read, c->marks_read);
	}
}

static void
chscan_end(CustomScanState *node)
{
	ChScanState *s = (ChScanState *) node;

	keep_counts(s);
	if (s->scan)
		table_endscan(s->scan);
	s->scan = NULL;
}

static void
chscan_rescan(CustomScanState *node)
{
	ChScanState *s = (ChScanState *) node;

	if (s->scan)
		table_rescan(s->scan, NULL);
	ExecScanReScan(&node->ss);
}

static Size
chscan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	ChScanState *s = (ChScanState *) node;

	return MAXALIGN(sizeof(ChScanShared)) +
		table_parallelscan_estimate(s->rel, node->ss.ps.state->es_snapshot);
}

static void
chscan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate)
{
	ChScanState *s = (ChScanState *) node;

	s->shared = coordinate;
	pg_atomic_init_u64(&s->shared->parts_pruned, 0);
	pg_atomic_init_u64(&s->shared->units_passed, 0);
	pg_atomic_init_u64(&s->shared->marks, 0);
	pg_atomic_init_u64(&s->shared->marks_read, 0);
	s->pscan = (ParallelTableScanDesc) ((char *) coordinate + MAXALIGN(sizeof(ChScanShared)));
	table_parallelscan_initialize(s->rel, s->pscan, node->ss.ps.state->es_snapshot);
}

static void
chscan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate)
{
	ChScanState *s = (ChScanState *) node;

	table_parallelscan_reinitialize(s->rel, s->pscan);
}

static void
chscan_initialize_worker(CustomScanState *node, shm_toc *toc, void *coordinate)
{
	ChScanState *s = (ChScanState *) node;

	s->shared = coordinate;
	s->pscan = (ParallelTableScanDesc) ((char *) coordinate + MAXALIGN(sizeof(ChScanShared)));
}

/* Before the DSM goes: the workers' counts, for EXPLAIN ANALYZE */
static void
chscan_shutdown(CustomScanState *node)
{
	ChScanState *s = (ChScanState *) node;

	keep_counts(s);
	if (s->shared && !IsParallelWorker())
	{
		s->others.parts_pruned = pg_atomic_read_u64(&s->shared->parts_pruned);
		s->others.units_passed = pg_atomic_read_u64(&s->shared->units_passed);
		s->others.marks = pg_atomic_read_u64(&s->shared->marks);
		s->others.marks_read = pg_atomic_read_u64(&s->shared->marks_read);
		s->shared = NULL;
	}
}

static void
chscan_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ChScanState *s = (ChScanState *) node;
	TupleDesc	desc = RelationGetDescr(s->rel);
	StringInfoData cols;

	initStringInfo(&cols);
	for (int i = 0; i < s->natts; i++)
		appendStringInfo(&cols, "%s%s", i > 0 ? ", " : "",
						 quote_identifier(NameStr(TupleDescAttr(desc, s->attnums[i] - 1)->attname)));
	if (s->need_tid)
		appendStringInfo(&cols, "%sctid", s->natts > 0 ? ", " : "");
	ExplainPropertyText("Columns", s->natts > 0 || s->need_tid ? cols.data : "none", es);
	if (s->pruning != NIL)
	{
		List	   *context = set_deparse_context_plan(es->deparse_cxt, node->ss.ps.plan, ancestors);
		Node	   *expr = (Node *) make_ands_explicit(s->pruning);

		ExplainPropertyText("Pruning", deparse_expression(expr, context, list_length(es->rtable) > 1, false), es);
	}
	if (es->analyze)
	{
		ChScanCounts c = s->counts;
		ChExternalKind kind;
		/* what a unit and a part are: an engine's granules and parts, a table's of files its files' (§2.17) */
		bool		external = ch_is_external_rel(s->rel, &kind);
		const char *units = !external ? "Granules" : kind == CH_EXTERNAL_ARROW ? "Record Batches" : "Row Groups";
		const char *parts = !external ? "parts" : "files";

		if (s->scan && !s->counted)
			c = *ch_scan_counts(s->scan);
		c.parts_pruned += s->others.parts_pruned;
		c.units_passed += s->others.units_passed;
		c.marks += s->others.marks;
		c.marks_read += s->others.marks_read;
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str, "%s: %lld read of %lld", units, (long long) c.marks_read, (long long) c.marks);
			if (c.parts_pruned > 0)
				appendStringInfo(es->str, ", %s pruned: %lld", parts, (long long) c.parts_pruned);
			appendStringInfoChar(es->str, '\n');
		}
		else
		{
			ExplainPropertyInteger(psprintf("%s Read", units), NULL, c.marks_read, es);
			ExplainPropertyInteger(units, NULL, c.marks, es);
			ExplainPropertyInteger(external ? "Files Pruned" : "Parts Pruned", NULL, c.parts_pruned, es);
		}
	}
}

void
ch_chscan_init(void)
{
	RegisterCustomScanMethods(&chscan_plan_methods);
	prev_set_rel_pathlist = set_rel_pathlist_hook;
	set_rel_pathlist_hook = ch_set_rel_pathlist;
	prev_planner = planner_hook;
	planner_hook = ch_planner;
}
