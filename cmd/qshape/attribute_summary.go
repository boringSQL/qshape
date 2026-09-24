package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/boringsql/qshape"
)

const (
	maxPartialLines        = 20
	maxUnattributedPerLine = 8
)

type attrStats struct {
	withParams       int
	withoutParams    int
	explainErrors    int
	fullyAttributed  int
	autoFillable     int
	totalParams      int
	attributedParams int
	exact            int
	heuristic        int
	partial          []partialCluster
}

type partialCluster struct {
	fingerprint  string
	attributed   int
	total        int
	unattributed []int
}

// add records one attributed cluster. "Fully attributed" means no entry is
// none; "auto-fillable" means every entry is exact.
func (s *attrStats) add(fingerprint string, params []qshape.ParamAttribution) {
	s.withParams++
	s.totalParams += len(params)
	fully, exactAll := true, true
	attributed := 0
	var unattributed []int
	for _, p := range params {
		switch p.Confidence {
		case "none":
			fully, exactAll = false, false
			unattributed = append(unattributed, p.Position)
		case "exact":
			s.exact++
			attributed++
		default:
			s.heuristic++
			attributed++
		}
	}
	s.attributedParams += attributed
	if fully {
		s.fullyAttributed++
	} else {
		s.partial = append(s.partial, partialCluster{
			fingerprint:  fingerprint,
			attributed:   attributed,
			total:        len(params),
			unattributed: unattributed,
		})
	}
	if exactAll {
		s.autoFillable++
	}
}

func printAttrSummary(w io.Writer, s attrStats, verbose bool) {
	pct := 0
	if s.totalParams > 0 {
		pct = s.attributedParams * 100 / s.totalParams
	}
	fmt.Fprintf(w, "params: %d/%d attributed (%d%%) — exact %d, heuristic %d\n",
		s.attributedParams, s.totalParams, pct, s.exact, s.heuristic)
	fmt.Fprintf(w, "clusters: %d/%d fully attributed, %d/%d auto-fillable (%d without params, %d explain error)\n",
		s.fullyAttributed, s.withParams, s.autoFillable, s.withParams, s.withoutParams, s.explainErrors)

	shown := len(s.partial)
	capped := false
	if !verbose && shown > maxPartialLines {
		shown = maxPartialLines
		capped = true
	}
	for _, p := range s.partial[:shown] {
		fmt.Fprintf(w, "  %s  %d/%d  unattributed: %s\n", p.fingerprint, p.attributed, p.total, positionList(p.unattributed))
	}
	if capped {
		fmt.Fprintf(w, "  … and %d more (--verbose for all)\n", len(s.partial)-shown)
	}
}

func positionList(positions []int) string {
	shown, suffix := positions, ""
	if len(shown) > maxUnattributedPerLine {
		suffix = fmt.Sprintf(", … (+%d more)", len(shown)-maxUnattributedPerLine)
		shown = shown[:maxUnattributedPerLine]
	}
	parts := make([]string, len(shown))
	for i, p := range shown {
		parts[i] = fmt.Sprintf("$%d", p)
	}
	return strings.Join(parts, ", ") + suffix
}
