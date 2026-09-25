package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/boringsql/qshape"
)

// Plans in testdata/attribute are captured from real servers by
// scripts/capture-attr-plans.sh. Every version must give the same entries,
// except where noted.
func TestAttributeFixtures(t *testing.T) {
	const dir = "../../testdata/attribute"
	versions := []int{13, 14, 15, 16, 17, 18}
	want := map[string][]string{
		"interval":       {"1 events.created_at expression note=now() - $1"},
		"any_array":      {"1 events.account_id exact shape=array"},
		"limit_offset":   {"1 events.kind exact", "2 . exact kind=limit", "3 . exact kind=offset"},
		"in_list_bare":   {"1 events.kind exact", "2 events.kind exact"},
		"in_list_cast":   {"1 events.status exact", "2 events.status exact"},
		"cast_column":    {"1 events.status exact"},
		"like":           {"1 events.email exact"},
		"jsonb_contains": {"1 events.data exact"},
		"initplan":       {"1 accounts.status exact", "2 events.email exact"},
		// A correlated SubPlan prints the outer reference as `a.id`, not $N,
		// so its params are unambiguous on every version.
		"subplan": {"1 accounts.status exact", "2 events.kind exact"},
	}
	// PG<17 prints InitPlan 2's output as $1, colliding with the external $1.
	wantPre17 := map[string][]string{
		"initplan": {"1 . none", "2 events.email exact"},
	}

	raw, err := os.ReadFile(filepath.Join(dir, "cases.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		name, sql, _ := strings.Cut(line, "\t")
		names = append(names, name)
		for _, v := range versions {
			t.Run(fmt.Sprintf("%s/pg%d", name, v), func(t *testing.T) {
				plan, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("pg%d", v), name+".json"))
				if err != nil {
					t.Fatal(err)
				}
				exp, ok := wantPre17[name]
				if !ok || v >= 17 {
					exp = want[name]
				}
				positions, limits := paramsFromTree(sql)
				params := attributeFromPlan(plan, positions)
				markLimits(params, limits)
				if got := fixtureLines(params); !slices.Equal(got, exp) {
					t.Errorf("got  %q\nwant %q", got, exp)
				}
			})
		}
	}
	if len(names) != len(want) {
		t.Errorf("cases.tsv has %d cases, want has %d", len(names), len(want))
	}
}

func fixtureLines(params []qshape.ParamAttribution) []string {
	out := make([]string, 0, len(params))
	for _, a := range params {
		s := fmt.Sprintf("%d %s.%s %s", a.Position, a.Table, a.Column, a.Confidence)
		for _, f := range [][2]string{{"shape", a.Shape}, {"kind", a.Kind}, {"note", a.Note}} {
			if f[1] != "" {
				s += " " + f[0] + "=" + f[1]
			}
		}
		out = append(out, s)
	}
	return out
}
