package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/boringsql/qshape"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

type (
	attrCtx struct {
		byPosition map[int]*qshape.ParamAttribution
		// Below PG 17, ruleutils prints InitPlan outputs as `$N`, in the same
		// namespace as the query's own parameters, so those numbers are
		// ambiguous and never attributed.
		initPlanParams []int
	}

	tableRef struct {
		Schema string
		Table  string
	}

	planNode struct {
		NodeType     string          `json:"Node Type"`
		Schema       string          `json:"Schema"`
		RelationName string          `json:"Relation Name"`
		Alias        string          `json:"Alias"`
		Filter       string          `json:"Filter"`
		IndexCond    string          `json:"Index Cond"`
		HashCond     string          `json:"Hash Cond"`
		RecheckCond  string          `json:"Recheck Cond"`
		JoinFilter   string          `json:"Join Filter"`
		MergeCond    string          `json:"Merge Cond"`
		TIDCond      string          `json:"TID Cond"`
		SubplanName  string          `json:"Subplan Name"`
		Plans        json.RawMessage `json:"Plans"`
	}
)

func attributeCmd() *cobra.Command {
	var (
		inPath  string
		connStr string
		top     int
		verbose bool
	)
	cmd := &cobra.Command{
		Use:   "attribute",
		Short: "Attribute $N placeholders to table.column",
		Long: `Read a clusters.json, run EXPLAIN (GENERIC_PLAN) on each cluster's
canonical SQL, and attribute every $N placeholder to a table.column.

Every parameter gets an entry; unattributed ones are confidence:"none" rather
than aborting, and a cluster is never dropped. Writes the input to stdout
with a "params" array added to each cluster.`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runAttribute(inPath, connStr, top, verbose)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input clusters.json (default: stdin)")
	cmd.Flags().StringVar(&connStr, "conn", "", "PostgreSQL connection string (required)")
	cmd.Flags().IntVar(&top, "top", 0, "only attribute the top N clusters (0 = all)")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "print every partially attributed cluster in the summary")
	_ = cmd.MarkFlagRequired("conn")
	return cmd
}

func runAttribute(inPath, connStr string, top int, verbose bool) error {
	var r io.Reader = os.Stdin
	if inPath != "" {
		f, err := os.Open(inPath)
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	}
	var doc clustersDoc
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return fmt.Errorf("decode clusters.json: %w", err)
	}
	if err := validateSchemaVersion(&doc); err != nil {
		return err
	}
	doc.SchemaVersion = currentSchemaVersion

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	cache := newTypecastCache(conn)
	var stats attrStats
	for i := range doc.Clusters {
		if top > 0 && i >= top {
			break
		}
		c := &doc.Clusters[i]
		if c.Fingerprint == "" || c.Canonical == "" {
			continue
		}
		canonical, params, explainErr := attributeCluster(ctx, conn, cache, c.Canonical)
		// Positions refer to the re-normalised canonical, so persist it even
		// when EXPLAIN failed.
		c.Canonical = canonical
		if explainErr != nil {
			stats.explainErrors++
		}
		if len(params) == 0 {
			stats.withoutParams++
			// Drop params from an earlier run: they refer to the old canonical.
			c.Params = nil
			continue
		}
		c.Params = params
		stats.add(c.Fingerprint, params)
	}

	printAttrSummary(os.Stderr, stats, verbose)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(doc)
}

// attributeCluster re-normalises canonical, EXPLAINs it, and returns the
// canonical it used plus one entry per parameter.
func attributeCluster(ctx context.Context, conn *pgx.Conn, cache *typecastCache, canonical string) (string, []qshape.ParamAttribution, error) {
	// Re-normalise so clusters.json written by an older qshape version picks
	// up current reshape fixes (extract-field recovery, param renumbering).
	// Fall back to the stored form if parsing fails
	if renormed, err := qshape.Normalize(canonical); err == nil {
		canonical = renormed
	}
	positions := paramPositions(canonical)
	if len(positions) == 0 {
		return canonical, nil, nil
	}
	explainSQL := castFuncParamRefs(ctx, cache, canonical)
	// PREPARE + EXPLAIN EXECUTE so Postgres sets up a parameter context
	// for the $N placeholders. Works on any PG version (GENERIC_PLAN alone
	// requires 16+, and the simple-query parser rejects bare $N otherwise).
	// Types come from the typecast pass we just ran; NULL values satisfy
	// EXECUTE's arity requirement. nparams is the highest position, which
	// equals the count only when positions are contiguous (always true after
	// renumbering; on the fallback path a gap makes PREPARE fail).
	nparams := positions[len(positions)-1]
	nulls := "NULL"
	for i := 1; i < nparams; i++ {
		nulls += ", NULL"
	}
	// force_generic_plan keeps $N in the plan output instead of inlining
	// the NULL arguments. Without it Postgres produces a custom plan with
	// `WHERE col = NULL` filters that walkPlan can't attribute
	script := "SET LOCAL plan_cache_mode = force_generic_plan;\n"
	script += "PREPARE _qshape_tmp AS " + explainSQL + ";\n"
	script += "EXPLAIN (FORMAT JSON) EXECUTE _qshape_tmp(" + nulls + ");\n"
	script += "DEALLOCATE _qshape_tmp;"
	// SET LOCAL only applies inside a transaction — wrap the whole script
	script = "BEGIN;\n" + script + "\nCOMMIT;"
	planJSON, err := readPlanJSON(ctx, conn, script)
	if err != nil {
		// A mid-batch error aborts the BEGIN'd transaction and skips the
		// trailing COMMIT in the same simple-query batch, so the connection
		// stays in aborted state and every following cluster fails with
		// 25P02. ROLLBACK resets it before the next call.
		_, _ = conn.Exec(ctx, "ROLLBACK")
		_, _ = conn.Exec(ctx, "DEALLOCATE IF EXISTS _qshape_tmp")
		return canonical, noneEntries(positions, err.Error()), err
	}
	return canonical, attributeFromPlan(planJSON, positions), nil
}

// attributeFromPlan turns an EXPLAIN (FORMAT JSON) result into one entry per
// canonical position.
func attributeFromPlan(planJSON []byte, positions []int) []qshape.ParamAttribution {
	var plans []struct {
		Plan json.RawMessage `json:"Plan"`
	}
	if err := json.Unmarshal(planJSON, &plans); err != nil {
		return noneEntries(positions, err.Error())
	}
	if len(plans) == 0 {
		return noneEntries(positions, "")
	}
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	walkPlan(plans[0].Plan, ctx)
	for _, p := range ctx.initPlanParams {
		delete(ctx.byPosition, p)
	}
	return fillPositions(ctx.byPosition, positions)
}

// readPlanJSON runs a multi-statement script (PREPARE; EXPLAIN; DEALLOCATE)
// through simple-query protocol and returns the first text cell of the
// first result-bearing statement. pgx's Query path sanitises $N against
// bound args; here we send the script verbatim
func readPlanJSON(ctx context.Context, conn *pgx.Conn, script string) ([]byte, error) {
	mrr := conn.PgConn().Exec(ctx, script)
	defer mrr.Close()
	var out []byte
	for mrr.NextResult() {
		rr := mrr.ResultReader()
		for rr.NextRow() {
			vals := rr.Values()
			if len(vals) > 0 && out == nil {
				out = append([]byte(nil), vals[0]...)
			}
		}
		if _, err := rr.Close(); err != nil {
			return nil, err
		}
	}
	if err := mrr.Close(); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, fmt.Errorf("no rows returned")
	}
	return out, nil
}

// maxParamNumber returns the highest $N in sql, or 0 if none.
func maxParamNumber(sql string) int {
	hi := 0
	for _, n := range scanParams(sql) {
		hi = max(hi, n)
	}
	return hi
}

func walkPlan(raw json.RawMessage, ctx *attrCtx) {
	if len(raw) == 0 {
		return
	}
	var n planNode
	if err := json.Unmarshal(raw, &n); err != nil {
		return
	}

	// Track alias → table mapping so we can resolve `u.id = $1` to users.id.
	// Function Scan on a system view like pg_catalog.pg_settings leaves
	// RelationName empty but still sets Alias; use the alias as the table name
	// so conds like `(name = $1)` attribute to pg_settings.name. A CTE or
	// Subquery Scan alias is not a table statistics exist for, so those stay
	// heuristic.
	if _, returns, ok := strings.Cut(n.SubplanName, "(returns "); ok {
		ctx.initPlanParams = append(ctx.initPlanParams, scanParams(returns)...)
	}
	aliasToTable := map[string]tableRef{}
	var fallbackSchema, fallbackTable, fallbackConfidence string
	if n.RelationName != "" {
		t := tableRef{Schema: n.Schema, Table: n.RelationName}
		aliasToTable[n.RelationName] = t
		if n.Alias != "" && n.Alias != n.RelationName {
			aliasToTable[n.Alias] = t
		}
		fallbackSchema, fallbackTable, fallbackConfidence = n.Schema, n.RelationName, "exact"
	} else if n.Alias != "" {
		t := tableRef{Schema: n.Schema, Table: n.Alias}
		aliasToTable[n.Alias] = t
		fallbackSchema, fallbackTable, fallbackConfidence = n.Schema, n.Alias, "heuristic"
		if n.NodeType == "Function Scan" {
			fallbackConfidence = "exact"
		}
	}

	for _, cond := range []string{
		n.IndexCond, n.HashCond, n.Filter, n.RecheckCond, n.JoinFilter,
		n.MergeCond, n.TIDCond,
	} {
		if cond == "" {
			continue
		}
		attributeCond(cond, aliasToTable, fallbackSchema, fallbackTable, fallbackConfidence, ctx)
	}

	if len(n.Plans) > 0 {
		var children []json.RawMessage
		if err := json.Unmarshal(n.Plans, &children); err == nil {
			for _, c := range children {
				walkPlan(c, ctx)
			}
		}
	}
}

func attributeCond(cond string, aliases map[string]tableRef, fallbackSchema, fallbackTable, fallbackConfidence string, ctx *attrCtx) {
	forEachComparison(cond, func(left, right string) {
		l := classifyOperand(left)
		r := classifyOperand(right)
		switch {
		case l.kind == operandColumn && r.kind != operandColumn && r.kind != operandOther:
			emitComparison(l, r, aliases, fallbackSchema, fallbackTable, fallbackConfidence, ctx)
		case r.kind == operandColumn && l.kind != operandColumn && l.kind != operandOther:
			emitComparison(r, l, aliases, fallbackSchema, fallbackTable, fallbackConfidence, ctx)
		}
	})
}

// emitComparison records the parameter(s) on the non-column side against the
// column side.
func emitComparison(col, other operand, aliases map[string]tableRef, fallbackSchema, fallbackTable, fallbackConfidence string, ctx *attrCtx) {
	ref, confidence := resolveColumn(col.alias, aliases, fallbackSchema, fallbackTable, fallbackConfidence)
	column := col.column
	if ref.Table == "" {
		column = ""
	}

	a := qshape.ParamAttribution{Schema: ref.Schema, Table: ref.Table, Column: column, Confidence: confidence}
	switch other.kind {
	case operandArrayParam:
		a.Shape = "array"
	case operandExpr:
		a.Note = other.text
		if confidence == "exact" && !exprHasOtherColumn(other.text) {
			a.Confidence = "expression"
		} else {
			a.Confidence = "none"
		}
	}
	for _, pos := range other.poss {
		a.Position = pos
		ctx.record(a)
	}
}

// resolveColumn maps an operand qualifier to a table. An empty qualifier on a
// scan node is PG pinning the column to that scan (exact); a qualifier that
// doesn't resolve is a best-effort guess (heuristic).
func resolveColumn(alias string, aliases map[string]tableRef, fallbackSchema, fallbackTable, fallbackConfidence string) (tableRef, string) {
	if ref, ok := aliases[alias]; ok {
		return ref, "exact"
	}
	if fallbackTable == "" {
		return tableRef{}, "none"
	}
	ref := tableRef{Schema: fallbackSchema, Table: fallbackTable}
	if alias == "" {
		return ref, fallbackConfidence
	}
	return ref, "heuristic"
}

// confidenceRank orders attribution confidence for merging hits across plan
// nodes; "none" ranks 0.
var confidenceRank = map[string]int{"exact": 3, "expression": 2, "heuristic": 1}

func (ctx *attrCtx) record(a qshape.ParamAttribution) {
	if prev, ok := ctx.byPosition[a.Position]; ok && confidenceRank[prev.Confidence] >= confidenceRank[a.Confidence] {
		return
	}
	if a.Confidence == "none" && a.Note == "" {
		// fillPositions emits the bare none entry; don't shadow a real one.
		return
	}
	ctx.byPosition[a.Position] = &a
}
