package main

import (
	"testing"

	"github.com/boringsql/qshape"
)

type wantAttr struct {
	pos        int
	table      string
	column     string
	shape      string
	confidence string
	note       string
}

func runCond(t *testing.T, cond string) map[int]*qshape.ParamAttribution {
	t.Helper()
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	attributeCond(cond, nil, "public", "events", "exact", ctx)
	return ctx.byPosition
}

// Conditions are verbatim plan text captured from PG 16.15 and 18.6.
func TestScannerCases(t *testing.T) {
	cases := []struct {
		name string
		cond string
		want []wantAttr
	}{
		{
			name: "interval expression",
			cond: "(created_at > (now() - $1))",
			want: []wantAttr{{1, "events", "created_at", "", "expression", "now() - $1"}},
		},
		{
			name: "array param",
			cond: "(account_id = ANY ($1))",
			want: []wantAttr{{1, "events", "account_id", "array", "exact", ""}},
		},
		{
			name: "IN list bare",
			cond: "(status = ANY (ARRAY[$1, $2]))",
			want: []wantAttr{
				{1, "events", "status", "", "exact", ""},
				{2, "events", "status", "", "exact", ""},
			},
		},
		{
			name: "IN list drops non-param elements",
			cond: "(status = ANY (ARRAY[($1 + 1), $2]))",
			want: []wantAttr{{2, "events", "status", "", "exact", ""}},
		},
		{
			name: "IN list cast on last element",
			cond: "(id = ANY (ARRAY[$1, ($2)::bigint]))",
			want: []wantAttr{
				{1, "events", "id", "", "exact", ""},
				{2, "events", "id", "", "exact", ""},
			},
		},
		{
			name: "bare ARRAY outside ANY is an expression",
			cond: "(tags @> ARRAY[$1])",
			want: []wantAttr{{1, "events", "tags", "", "expression", "ARRAY[$1]"}},
		},
		{
			name: "cast on last term stays in the note",
			cond: "(created_at > (now() - ($1)::interval))",
			want: []wantAttr{{1, "events", "created_at", "", "expression", "now() - ($1)::interval"}},
		},
		{
			name: "like",
			cond: "(email ~~ $1)",
			want: []wantAttr{{1, "events", "email", "", "exact", ""}},
		},
		{
			name: "jsonb contains",
			cond: "(data @> $1)",
			want: []wantAttr{{1, "events", "data", "", "exact", ""}},
		},
		{
			name: "array overlap",
			cond: "(tags && $1)",
			want: []wantAttr{{1, "events", "tags", "", "exact", ""}},
		},
		{
			name: "is distinct from",
			cond: "((status)::text IS DISTINCT FROM $1)",
			want: []wantAttr{{1, "events", "status", "", "exact", ""}},
		},
		{
			name: "function on column is not a column",
			cond: "(lower(email) = $1)",
			want: nil,
		},
		{
			name: "SQL value function is not a column",
			cond: "(CURRENT_ROLE = $1)",
			want: nil,
		},
		{
			name: "cast column",
			cond: "((status)::text = $1)",
			want: []wantAttr{{1, "events", "status", "", "exact", ""}},
		},
		{
			name: "array param ALL no space",
			cond: "(account_id <> ALL($1))",
			want: []wantAttr{{1, "events", "account_id", "array", "exact", ""}},
		},
		{
			name: "IN list cast",
			cond: "((status)::text = ANY ((ARRAY[$1, $2, $3])::text[]))",
			want: []wantAttr{
				{1, "events", "status", "", "exact", ""},
				{2, "events", "status", "", "exact", ""},
				{3, "events", "status", "", "exact", ""},
			},
		},
		{
			name: "ilike",
			cond: "(email ~~* $1)",
			want: []wantAttr{{1, "events", "email", "", "exact", ""}},
		},
		{
			name: "is not distinct from",
			cond: "(NOT ((status)::text IS DISTINCT FROM $1))",
			want: []wantAttr{{1, "events", "status", "", "exact", ""}},
		},
		{
			name: "inner cast is an expression, not a bare param",
			cond: "(created_at > (($1)::date + 1))",
			want: []wantAttr{{1, "events", "created_at", "", "expression", "($1)::date + 1"}},
		},
		{
			name: "current date expression",
			cond: "(created_at > (CURRENT_DATE - $1))",
			want: []wantAttr{{1, "events", "created_at", "", "expression", "CURRENT_DATE - $1"}},
		},
		{
			name: "jsonb arrow is not a column",
			cond: "((data ->> 'k'::text) = $1)",
			want: nil,
		},
		{
			name: "arithmetic column not attributed",
			cond: "((amount + $1) > '5'::numeric)",
			want: nil,
		},
		{
			name: "two params one expression",
			cond: "((access_sha = $2) AND (access_hash = hashtext($1)))",
			want: []wantAttr{
				{1, "events", "access_hash", "", "expression", "hashtext($1)"},
				{2, "events", "access_sha", "", "exact", ""},
			},
		},
		{
			name: "expression with other column is none with note",
			cond: "(filter_col = (other_col - $1))",
			want: []wantAttr{{1, "events", "filter_col", "", "none", "other_col - $1"}},
		},
		{
			name: "subplan reference is none with note",
			cond: "((account_id = (InitPlan 1).col1) AND (tenant_id = ((InitPlan 2).col1 - $1)))",
			want: []wantAttr{{1, "events", "tenant_id", "", "none", "(InitPlan 2).col1 - $1"}},
		},
		{
			name: "distance order by is not a split",
			cond: "(data <-> $1)",
			want: nil,
		},
		{
			name: "unary bitwise not is not a split",
			cond: "(~flags = $1)",
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runCond(t, tc.cond)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d entries %+v, want %d", len(got), got, len(tc.want))
			}
			for _, w := range tc.want {
				a, ok := got[w.pos]
				if !ok {
					t.Errorf("missing entry for $%d", w.pos)
					continue
				}
				if a.Table != w.table || a.Column != w.column || a.Shape != w.shape ||
					a.Confidence != w.confidence || a.Note != w.note {
					t.Errorf("$%d = %+v, want %+v", w.pos, a, w)
				}
			}
		})
	}
}
