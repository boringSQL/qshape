package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"

	"github.com/boringsql/qshape"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

type (
	attrCtx struct {
		byPosition map[int]*qshape.ParamAttribution
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
		SubplanName  string          `json:"Subplan Name"`
		Plans        json.RawMessage `json:"Plans"`
	}
)

var (
	// column op $N or $N op column — alias.column optional
	paramCondRE = regexp.MustCompile(`(?:\(?(\w+)\.)?(\w+)\s*(?:=|<|>|<=|>=|<>|!=)\s*\$(\d+)|\$(\d+)\s*(?:=|<|>|<=|>=|<>|!=)\s*(?:\(?(\w+)\.)?(\w+)`)
	// column IN ($N, $M, ...) — capture only the first param and the column
	paramInRE = regexp.MustCompile(`(?:\(?(\w+)\.)?(\w+)\s+(?:=\s*ANY\s*\()?IN\s*\(\s*\$(\d+)`)
)

func attributeCmd() *cobra.Command {
	var (
		inPath  string
		connStr string
		top     int
	)
	cmd := &cobra.Command{
		Use:   "attribute",
		Short: "Attribute $N placeholders to table.column",
		Long: `Read a clusters.json, run EXPLAIN (GENERIC_PLAN) on each cluster's
canonical SQL, and attribute every $N placeholder to a table.column.

Attribution failures are recorded as confidence:"none" rather than
aborting. Writes the input to stdout with a "params" array added to
each cluster.`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runAttribute(inPath, connStr, top)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input clusters.json (default: stdin)")
	cmd.Flags().StringVar(&connStr, "conn", "", "PostgreSQL connection string (required)")
	cmd.Flags().IntVar(&top, "top", 0, "only attribute the top N clusters (0 = all)")
	_ = cmd.MarkFlagRequired("conn")
	return cmd
}

func runAttribute(inPath, connStr string, top int) error {
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

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	cache := newTypecastCache(conn)
	attributed, skipped := 0, 0
	for i := range doc.Clusters {
		if top > 0 && i >= top {
			break
		}
		c := &doc.Clusters[i]
		if c.Fingerprint == "" || c.Canonical == "" {
			continue
		}
		params, err := attributeCluster(ctx, conn, cache, c.Canonical)
		if err != nil {
			skipped++
			c.Params = []qshape.ParamAttribution{{Confidence: "none", Note: err.Error()}}
			continue
		}
		if len(params) == 0 {
			skipped++
			continue
		}
		c.Params = params
		attributed++
	}

	fmt.Fprintf(os.Stderr, "attributed %d clusters, %d skipped\n", attributed, skipped)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(doc)
}

func attributeCluster(ctx context.Context, conn *pgx.Conn, cache *typecastCache, canonical string) ([]qshape.ParamAttribution, error) {
	// Re-normalise so clusters.json written by an older qshape version picks
	// up current reshape fixes (extract-field recovery, param renumbering).
	// Fall back to the stored form if parsing fails
	if renormed, err := qshape.Normalize(canonical); err == nil {
		canonical = renormed
	}
	explainSQL := castFuncParamRefs(ctx, cache, canonical)
	// PREPARE + EXPLAIN EXECUTE so Postgres sets up a parameter context
	// for the $N placeholders. Works on any PG version (GENERIC_PLAN alone
	// requires 16+, and the simple-query parser rejects bare $N otherwise).
	// Types come from the typecast pass we just ran; NULL values satisfy
	// EXECUTE's arity requirement without affecting the plan
	nparams := maxParamNumber(explainSQL)
	nulls := "NULL"
	for i := 1; i < nparams; i++ {
		nulls += ", NULL"
	}
	// force_generic_plan keeps $N in the plan output instead of inlining
	// the NULL arguments. Without it Postgres produces a custom plan with
	// `WHERE col = NULL` filters that walkPlan can't attribute
	script := "SET LOCAL plan_cache_mode = force_generic_plan;\n"
	script += "PREPARE _qshape_tmp AS " + explainSQL + ";\n"
	if nparams > 0 {
		script += "EXPLAIN (FORMAT JSON) EXECUTE _qshape_tmp(" + nulls + ");\n"
	} else {
		script += "EXPLAIN (FORMAT JSON) EXECUTE _qshape_tmp;\n"
	}
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
		return nil, err
	}

	var plans []struct {
		Plan json.RawMessage `json:"Plan"`
	}
	if err := json.Unmarshal(planJSON, &plans); err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return nil, nil
	}

	c := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	walkPlan(plans[0].Plan, "", "", c)

	out := make([]qshape.ParamAttribution, 0, len(c.byPosition))
	for _, p := range c.byPosition {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Position < out[j].Position })
	return out, nil
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

// maxParamNumber scans sql for `$N` tokens and returns the highest N seen,
// or 0 if none. Skips $-tags inside string and dollar-quoted contexts
func maxParamNumber(sql string) int {
	max := 0
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch c {
		case '\'':
			// skip to matching quote, honoring doubled ''
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i += 2
						continue
					}
					break
				}
				i++
			}
		case '$':
			j := i + 1
			n := 0
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				n = n*10 + int(sql[j]-'0')
				j++
			}
			if j > i+1 {
				if n > max {
					max = n
				}
				i = j - 1
			}
		}
	}
	return max
}

func walkPlan(raw json.RawMessage, parentSchema, parentTable string, ctx *attrCtx) {
	if len(raw) == 0 {
		return
	}
	var n planNode
	if err := json.Unmarshal(raw, &n); err != nil {
		return
	}

	// Track alias → table mapping so we can resolve `u.id = $1` to users.id.
	// Function Scan on a system view like pg_catalog.pg_settings leaves
	// RelationName empty but still sets Alias; use Alias as the table name
	// so conds like `(name = $1)` attribute to pg_settings.name.
	aliasToTable := map[string]tableRef{}
	fallbackTable := n.RelationName
	fallbackSchema := n.Schema
	if n.RelationName != "" {
		t := tableRef{Schema: n.Schema, Table: n.RelationName}
		aliasToTable[n.RelationName] = t
		if n.Alias != "" && n.Alias != n.RelationName {
			aliasToTable[n.Alias] = t
		}
	} else if n.Alias != "" {
		// Function Scan / Values Scan: no Relation Name but Alias names the
		// logical target (e.g. pg_settings). Attribute to the alias.
		t := tableRef{Schema: n.Schema, Table: n.Alias}
		aliasToTable[n.Alias] = t
		fallbackTable = n.Alias
	}

	for _, cond := range []string{n.IndexCond, n.HashCond, n.Filter, n.RecheckCond, n.JoinFilter, n.MergeCond} {
		if cond == "" {
			continue
		}
		attributeCond(cond, aliasToTable, fallbackSchema, fallbackTable, ctx)
	}

	if len(n.Plans) > 0 {
		var children []json.RawMessage
		if err := json.Unmarshal(n.Plans, &children); err == nil {
			for _, c := range children {
				walkPlan(c, n.Schema, n.RelationName, ctx)
			}
		}
	}
}

func attributeCond(cond string, aliases map[string]tableRef, fallbackSchema, fallbackTable string, ctx *attrCtx) {
	for _, m := range paramCondRE.FindAllStringSubmatch(cond, -1) {
		// Two alternatives in the regex: [1]=alias,[2]=col,[3]=pos OR [4]=pos,[5]=alias,[6]=col
		var aliasOrTable, col, posStr string
		if m[3] != "" {
			aliasOrTable, col, posStr = m[1], m[2], m[3]
		} else {
			aliasOrTable, col, posStr = m[5], m[6], m[4]
		}
		recordParam(aliasOrTable, col, posStr, aliases, fallbackSchema, fallbackTable, ctx)
	}
	for _, m := range paramInRE.FindAllStringSubmatch(cond, -1) {
		recordParam(m[1], m[2], m[3], aliases, fallbackSchema, fallbackTable, ctx)
	}
}

func recordParam(aliasOrTable, col, posStr string, aliases map[string]tableRef, fallbackSchema, fallbackTable string, ctx *attrCtx) {
	pos, err := strconv.Atoi(posStr)
	if err != nil {
		return
	}
	// Prefer higher-confidence attribution if we already saw this param.
	existing, already := ctx.byPosition[pos]
	if already && existing.Confidence == "exact" {
		return
	}

	ref, ok := aliases[aliasOrTable]
	confidence := "exact"
	if !ok {
		if fallbackTable != "" {
			ref = tableRef{Schema: fallbackSchema, Table: fallbackTable}
			// An unqualified column in plan text (e.g. `Filter: (id = $1)`
			// on an Index Scan over `session`) is PG telling us exactly
			// which scan node the column belongs to — not a guess. Only
			// downgrade to heuristic when we saw a qualifier that didn't
			// resolve (stale alias, schema-qualified name we didn't
			// track, or a subplan reference).
			if aliasOrTable == "" {
				confidence = "exact"
			} else {
				confidence = "heuristic"
			}
		} else {
			confidence = "none"
		}
	}

	ctx.byPosition[pos] = &qshape.ParamAttribution{
		Position:   pos,
		Schema:     ref.Schema,
		Table:      ref.Table,
		Column:     col,
		Confidence: confidence,
	}
}
