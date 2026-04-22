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

	attributed, skipped := 0, 0
	for i := range doc.Clusters {
		if top > 0 && i >= top {
			break
		}
		c := &doc.Clusters[i]
		if c.Fingerprint == "" || c.Canonical == "" {
			continue
		}
		params, err := attributeCluster(ctx, conn, c.Canonical)
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

func attributeCluster(ctx context.Context, conn *pgx.Conn, canonical string) ([]qshape.ParamAttribution, error) {
	var planJSON []byte
	row := conn.QueryRow(ctx, "EXPLAIN (GENERIC_PLAN, FORMAT JSON) "+canonical)
	if err := row.Scan(&planJSON); err != nil {
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

func walkPlan(raw json.RawMessage, parentSchema, parentTable string, ctx *attrCtx) {
	if len(raw) == 0 {
		return
	}
	var n planNode
	if err := json.Unmarshal(raw, &n); err != nil {
		return
	}

	// Track alias → table mapping so we can resolve `u.id = $1` to users.id
	aliasToTable := map[string]tableRef{}
	if n.RelationName != "" {
		t := tableRef{Schema: n.Schema, Table: n.RelationName}
		aliasToTable[n.RelationName] = t
		if n.Alias != "" && n.Alias != n.RelationName {
			aliasToTable[n.Alias] = t
		}
	}

	for _, cond := range []string{n.IndexCond, n.HashCond, n.Filter, n.RecheckCond, n.JoinFilter, n.MergeCond} {
		if cond == "" {
			continue
		}
		attributeCond(cond, aliasToTable, n.Schema, n.RelationName, ctx)
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
		// Bare column without an alias — attribute to the current relation.
		if fallbackTable != "" {
			ref = tableRef{Schema: fallbackSchema, Table: fallbackTable}
			confidence = "heuristic"
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
