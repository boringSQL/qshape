package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/boringsql/qshape"
)

func TestPrintAttrSummary(t *testing.T) {
	s := attrStats{
		withParams:       21,
		totalParams:      42,
		attributedParams: 20,
		exact:            20,
	}
	for i := range 21 {
		s.partial = append(s.partial, partialCluster{
			fingerprint:  fmt.Sprintf("fp%02d", i),
			total:        2,
			unattributed: []int{1, 2},
		})
	}

	var buf bytes.Buffer
	printAttrSummary(&buf, s, false)
	out := buf.String()
	if !strings.Contains(out, "params: 20/42 attributed (47%) — exact 20, expression 0, heuristic 0") {
		t.Errorf("missing params line:\n%s", out)
	}
	if !strings.Contains(out, "clusters: 0/21 fully attributed, 0/21 auto-fillable (0 without params, 0 explain error)") {
		t.Errorf("missing clusters line:\n%s", out)
	}
	if n := strings.Count(out, "unattributed:"); n != maxPartialLines {
		t.Errorf("printed %d partial lines, want %d:\n%s", n, maxPartialLines, out)
	}
	if !strings.Contains(out, "… and 1 more (--verbose for all)") {
		t.Errorf("missing cap line:\n%s", out)
	}

	buf.Reset()
	printAttrSummary(&buf, s, true)
	if n := strings.Count(buf.String(), "unattributed:"); n != 21 {
		t.Errorf("verbose printed %d partial lines, want 21:\n%s", n, buf.String())
	}
	if strings.Contains(buf.String(), "more (--verbose for all)") {
		t.Errorf("verbose should not cap:\n%s", buf.String())
	}

	if got := positionList([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); !strings.HasSuffix(got, "… (+2 more)") {
		t.Errorf("long position list not truncated: %q", got)
	}
}

// Auto-fillable means every entry is exact. An expression or heuristic hit
// still counts as attributed, but needs hand work, so it is excluded.
func TestAttrStatsAutoFillableExcludesExpressionAndHeuristic(t *testing.T) {
	var s attrStats
	s.add("fp", []qshape.ParamAttribution{
		{Position: 1, Confidence: "exact"},
		{Position: 2, Confidence: "heuristic"},
		{Position: 3, Confidence: "expression"},
	})
	if s.autoFillable != 0 {
		t.Errorf("autoFillable = %d, want 0", s.autoFillable)
	}
	if s.fullyAttributed != 1 {
		t.Errorf("fullyAttributed = %d, want 1 (no none entries)", s.fullyAttributed)
	}
	if s.exact != 1 || s.expression != 1 || s.heuristic != 1 {
		t.Errorf("counters = exact %d expression %d heuristic %d", s.exact, s.expression, s.heuristic)
	}
}
