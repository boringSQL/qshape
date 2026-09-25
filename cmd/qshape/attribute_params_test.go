package main

import (
	"maps"
	"slices"
	"testing"

	"github.com/boringsql/qshape"
)

func TestParamsFromTree(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		want   []int
		limits map[int]string
	}{
		{"gaps and dupes", "SELECT 1 FROM t WHERE a = $1 AND b = $3 AND c = $1", []int{1, 3}, nil},
		{"nested subquery", "SELECT 1 FROM t WHERE id IN (SELECT id FROM u WHERE x = $2)", []int{2}, nil},
		{"unparsable fallback", "SELECT $2 FROM", []int{1, 2}, nil},
		{"no params", "SELECT 1 FROM t", nil, nil},
		{
			"limit offset",
			"SELECT 1 FROM t WHERE a = $1 LIMIT $2 OFFSET $3",
			[]int{1, 2, 3}, map[int]string{2: "limit", 3: "offset"},
		},
		{
			"cast",
			"SELECT 1 FROM t LIMIT $1::bigint",
			[]int{1}, map[int]string{1: "limit"},
		},
		{
			"union arms",
			"(SELECT 1 FROM t LIMIT $1) UNION ALL (SELECT 1 FROM u OFFSET $2)",
			[]int{1, 2}, map[int]string{1: "limit", 2: "offset"},
		},
		{
			"cte and subquery",
			"WITH c AS (SELECT a FROM t LIMIT $1) SELECT a FROM (SELECT a FROM c OFFSET $2) s",
			[]int{1, 2}, map[int]string{1: "limit", 2: "offset"},
		},
		{
			"not a param",
			"SELECT 1 FROM t LIMIT ALL OFFSET $1 + 1",
			[]int{1}, map[int]string{},
		},
		{
			"shared limit wins",
			"SELECT 1 FROM t LIMIT $1 OFFSET $1",
			[]int{1}, map[int]string{1: "limit"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, limits := paramsFromTree(tc.in)
			if !slices.Equal(got, tc.want) {
				t.Errorf("paramsFromTree(%q) positions = %v, want %v", tc.in, got, tc.want)
			}
			if !maps.Equal(limits, tc.limits) {
				t.Errorf("paramsFromTree(%q) limits = %v, want %v", tc.in, limits, tc.limits)
			}
		})
	}
}

func TestNoneEntries(t *testing.T) {
	got := noneEntries([]int{1, 4}, "boom")
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(got), got)
	}
	if got[0].Position != 1 || got[1].Position != 4 {
		t.Errorf("wrong positions: %+v", got)
	}
	for _, a := range got {
		if a.Confidence != "none" || a.Note != "boom" {
			t.Errorf("wrong entry: %+v", a)
		}
		if a.Position == 0 {
			t.Errorf("position 0 entry must not exist: %+v", a)
		}
	}
}

// markLimits overwrites both the plan path (an exact column hit) and the
// EXPLAIN-error path (a none entry with a note), and leaves other positions
// untouched.
func TestMarkLimits(t *testing.T) {
	params := noneEntries([]int{1, 2, 3}, "boom")
	params[0] = qshape.ParamAttribution{Position: 1, Schema: "public", Table: "users", Column: "id", Confidence: "exact"}

	markLimits(params, map[int]string{1: "limit", 2: "offset"})

	want := []qshape.ParamAttribution{
		{Position: 1, Kind: "limit", Confidence: "exact"},
		{Position: 2, Kind: "offset", Confidence: "exact", Note: "boom"},
		{Position: 3, Confidence: "none", Note: "boom"},
	}
	for i := range want {
		if params[i] != want[i] {
			t.Errorf("entry %d = %+v, want %+v", i, params[i], want[i])
		}
	}
}
