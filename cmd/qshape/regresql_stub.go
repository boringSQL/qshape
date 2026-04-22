package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/boringsql/qshape"
	"github.com/spf13/cobra"
)

type fixtureDoc struct {
	Tables map[string]struct {
		Columns []string `json:"columns"`
		Rows    [][]any  `json:"rows"`
	} `json:"tables"`
}

var paramRE = regexp.MustCompile(`\$(\d+)`)

func regresqlStubCmd() *cobra.Command {
	var (
		inPath      string
		outDir      string
		top         int
		minCalls    int64
		fixturePath string
		samplesPer  int
	)
	cmd := &cobra.Command{
		Use:   "regresql-stub",
		Short: "Generate regresql sql/ + plans/ skeletons from clusters.json",
		Long: `Walk a clusters.json and emit regresql sql/ + plans/ skeletons for
the top N clusters.

Each cluster becomes a .sql file (canonical SQL with $N → :paramN) and
a YAML plan with REPLACE_ME test cases. If --fixture is set and the
clusters were first run through 'qshape attribute', plan YAMLs are
auto-populated with real sampled values.`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runRegresqlStub(inPath, outDir, top, minCalls, fixturePath, samplesPer)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input clusters.json (default: stdin)")
	cmd.Flags().StringVar(&outDir, "out", "regresql-stubs", "output directory")
	cmd.Flags().IntVar(&top, "top", 10, "number of top clusters to emit")
	cmd.Flags().Int64Var(&minCalls, "min-calls", 0, "skip clusters with total_calls <= this")
	cmd.Flags().StringVar(&fixturePath, "fixture", "", "fixturize-format JSON used to auto-fill plan YAMLs")
	cmd.Flags().IntVar(&samplesPer, "samples", 2, "test cases to emit per query (sampled from fixture if available)")
	return cmd
}

func loadFixture(path string) (*fixtureDoc, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var fix fixtureDoc
	if err := json.NewDecoder(f).Decode(&fix); err != nil {
		return nil, fmt.Errorf("decode fixture: %w", err)
	}
	return &fix, nil
}

// sampleValues returns up to n non-null values from schema.table.column.
// Returns nil if the table or column isn't present in the fixture.
func (f *fixtureDoc) sampleValues(schema, table, column string, n int) []any {
	if f == nil {
		return nil
	}
	keys := []string{schema + "." + table, table, "public." + table}
	var tbl *struct {
		Columns []string `json:"columns"`
		Rows    [][]any  `json:"rows"`
	}
	for _, k := range keys {
		if t, ok := f.Tables[k]; ok {
			t := t
			tbl = &t
			break
		}
	}
	if tbl == nil {
		return nil
	}
	idx := -1
	for i, c := range tbl.Columns {
		if c == column {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil
	}
	out := make([]any, 0, n)
	for _, row := range tbl.Rows {
		if idx >= len(row) {
			continue
		}
		v := row[idx]
		if v == nil {
			continue
		}
		out = append(out, v)
		if len(out) >= n {
			break
		}
	}
	return out
}

func runRegresqlStub(inPath, outDir string, top int, minCalls int64, fixturePath string, samplesPer int) error {
	// TODO: plans/ generation is temporarily disabled — only sql/ stubs are created
	_, _ = fixturePath, samplesPer
	// var fix *fixtureDoc
	// if fixturePath != "" {
	// 	var err error
	// 	fix, err = loadFixture(fixturePath)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// if samplesPer < 1 {
	// 	samplesPer = 1
	// }
	doc, err := loadClustersDoc(inPath)
	if err != nil {
		return err
	}

	sqlDir := filepath.Join(outDir, "sql")
	if err := os.MkdirAll(sqlDir, 0o755); err != nil {
		return err
	}
	// plansDir := filepath.Join(outDir, "plans")
	// if err := os.MkdirAll(plansDir, 0o755); err != nil {
	// 	return err
	// }

	emitted := 0
	for _, c := range doc.Clusters {
		if emitted >= top {
			break
		}
		if c.Fingerprint == "" {
			continue
		}
		if c.TotalCalls <= minCalls {
			continue
		}

		emitted++
		slug := stubSlug(emitted, c.Fingerprint)
		sql, _ := rewriteParams(c.Canonical)

		sqlPath := filepath.Join(sqlDir, slug+".sql")
		if err := writeSQLStub(sqlPath, slug, c, sql); err != nil {
			return err
		}
		// planPath := filepath.Join(plansDir, slug+"_"+slug+".yaml")
		// values := sampleValuesForParams(params, c.Params, fix, samplesPer)
		// if err := writePlanStub(planPath, params, values); err != nil {
		// 	return err
		// }
	}

	fmt.Fprintf(os.Stderr, "wrote %d stubs to %s\n", emitted, outDir)
	return nil
}

func stubSlug(rank int, fp string) string {
	prefix := strings.TrimPrefix(fp, "sha1:")
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}
	return fmt.Sprintf("q%02d-%s", rank, prefix)
}

// rewriteParams replaces $N with :paramN and returns the sorted unique
// param names discovered. $1 → :param1, $2 → :param2, ...
func rewriteParams(sql string) (string, []string) {
	seen := map[string]struct{}{}
	out := paramRE.ReplaceAllStringFunc(sql, func(m string) string {
		n := m[1:]
		name := "param" + n
		seen[name] = struct{}{}
		return ":" + name
	})
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool {
		ni, _ := strconv.Atoi(strings.TrimPrefix(names[i], "param"))
		nj, _ := strconv.Atoi(strings.TrimPrefix(names[j], "param"))
		return ni < nj
	})
	return out, names
}

func writeSQLStub(path, slug string, c qshape.Cluster, sql string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "-- name: %s\n", slug)
	fmt.Fprintf(&b, "-- Generated from qshape cluster %s\n", c.Fingerprint)
	fmt.Fprintf(&b, "-- Total calls (prod): %d across %d member variants\n", c.TotalCalls, len(c.Members))
	fmt.Fprintf(&b, "-- TODO: rename this slug, review canonical SQL, replace :paramN with meaningful names\n")
	b.WriteString(sql)
	if !strings.HasSuffix(sql, "\n") {
		b.WriteString("\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// writePlanStub maps paramN → list of sample values (one per test case).
// If values[p] is empty or absent, the plan uses REPLACE_ME.
func writePlanStub(path string, params []string, values map[string][]any) error {
	var b strings.Builder
	if len(params) == 0 {
		b.WriteString("\"1\": {}\n")
		return os.WriteFile(path, []byte(b.String()), 0o644)
	}

	numCases := 2
	for _, vs := range values {
		if len(vs) > numCases {
			numCases = len(vs)
		}
	}
	for i := 0; i < numCases; i++ {
		fmt.Fprintf(&b, "%q:\n", strconv.Itoa(i+1))
		for _, p := range params {
			vs := values[p]
			if i < len(vs) {
				fmt.Fprintf(&b, "  %s: %s\n", p, yamlScalar(vs[i]))
			} else {
				fmt.Fprintf(&b, "  %s: REPLACE_ME\n", p)
			}
		}
		b.WriteString("\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// sampleValuesForParams maps paramN → []sample-values, using the cluster's
// attribution + fixture. Params without attribution get no values (caller
// emits REPLACE_ME).
func sampleValuesForParams(paramNames []string, attrs []qshape.ParamAttribution, fix *fixtureDoc, n int) map[string][]any {
	out := map[string][]any{}
	if fix == nil || len(attrs) == 0 {
		return out
	}
	byPos := map[int]qshape.ParamAttribution{}
	for _, a := range attrs {
		byPos[a.Position] = a
	}
	for _, pname := range paramNames {
		posStr := strings.TrimPrefix(pname, "param")
		pos, err := strconv.Atoi(posStr)
		if err != nil {
			continue
		}
		a, ok := byPos[pos]
		if !ok || a.Table == "" || a.Column == "" {
			continue
		}
		vals := fix.sampleValues(a.Schema, a.Table, a.Column, n)
		if len(vals) > 0 {
			out[pname] = vals
		}
	}
	return out
}

// yamlScalar renders a sample value as a YAML scalar. We keep it simple —
// numbers and bools unquoted, strings double-quoted with escaping, nil → ~.
func yamlScalar(v any) string {
	switch x := v.(type) {
	case nil:
		return "~"
	case bool:
		return strconv.FormatBool(x)
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int, int64:
		return fmt.Sprintf("%d", x)
	case string:
		esc := strings.ReplaceAll(x, `\`, `\\`)
		esc = strings.ReplaceAll(esc, `"`, `\"`)
		return `"` + esc + `"`
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

func loadClustersDoc(path string) (*clustersDoc, error) {
	var r io.Reader = os.Stdin
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}
	var doc clustersDoc
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode clusters.json: %w", err)
	}
	if err := validateSchemaVersion(&doc); err != nil {
		return nil, err
	}
	return &doc, nil
}
