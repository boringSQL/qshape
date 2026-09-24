package main

import (
	"slices"
	"testing"
)

func TestParamPositions(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []int
	}{
		{"gaps and dupes", "SELECT 1 FROM t WHERE a = $1 AND b = $3 AND c = $1", []int{1, 3}},
		{"nested subquery", "SELECT 1 FROM t WHERE id IN (SELECT id FROM u WHERE x = $2)", []int{2}},
		{"unparsable fallback", "SELECT $2 FROM", []int{1, 2}},
		{"no params", "SELECT 1 FROM t", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := paramPositions(tc.in)
			if !slices.Equal(got, tc.want) {
				t.Errorf("paramPositions(%q) = %v, want %v", tc.in, got, tc.want)
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
