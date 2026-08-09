package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/boringsql/qshape"
	"github.com/spf13/cobra"
)

func sortsCmd() *cobra.Command {
	var (
		inPath   string
		top      int
		minSorts int
		asJSON   bool
		width    int
	)
	cmd := &cobra.Command{
		Use:   "sorts",
		Short: "Show which ORDER BY variants a statement is actually called with",
		Long: `Group clusters by their sort-insensitive cost key and report every distinct
ORDER BY observed in each, with its own calls and exec time.

pg_stat_statements gives one row per sort, so a statement issued with twenty
different orderings appears as twenty unrelated entries and none of them looks
dominant. This regroups them: one row per statement, then the orderings inside
it ranked by cost. That answers "which sort do I actually need to optimize",
which the raw top-N cannot.

Ordering counts are a floor. Fingerprinting ignores constants, so ORDER BY 1
and ORDER BY 2 fold together, and a member with no ORDER BY adds none.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSorts(cmd.OutOrStdout(), inPath, top, minSorts, asJSON, width)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input clusters.json (default: stdin)")
	cmd.Flags().IntVar(&top, "top", 10, "statements to show, ranked by total exec time (0 for all)")
	cmd.Flags().IntVar(&minSorts, "min-sorts", 2, "skip statements with fewer distinct orderings; 1 shows every statement")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the full profile as JSON (ignores --top and --min-sorts)")
	cmd.Flags().IntVar(&width, "width", 72, "truncate rendered ORDER BY text to this many characters")
	return cmd
}

func runSorts(w io.Writer, inPath string, top, minSorts int, asJSON bool, width int) error {
	doc, err := loadClustersDoc(inPath)
	if err != nil {
		return err
	}

	profiles := qshape.SortProfiles(doc.Clusters)

	// JSON is the machine-readable form, so it stays lossless: --top and
	// --min-sorts shape the human report only.
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(profiles)
	}

	// A statement with one ordering has nothing to choose between, so it is
	// noise here by default — the point of the command is the fragmented ones.
	kept := make([]qshape.SortProfile, 0, len(profiles))
	for _, p := range profiles {
		if p.DistinctSorts >= minSorts {
			kept = append(kept, p)
		}
	}
	if top > 0 && len(kept) > top {
		kept = kept[:top]
	}

	if len(kept) == 0 {
		fmt.Fprintf(w, "no statement has %d or more distinct orderings "+
			"(%d statement(s) in the input; --min-sorts 1 shows them all)\n",
			minSorts, len(profiles))
		return nil
	}

	var grand float64
	for _, p := range profiles {
		grand += p.TotalExecTimeMs
	}

	for i, p := range kept {
		if i > 0 {
			fmt.Fprintln(w)
		}
		share := ""
		if grand > 0 {
			share = fmt.Sprintf(" — %.1f%% of captured exec time", 100*p.TotalExecTimeMs/grand)
		}
		fmt.Fprintf(w, "#%d  %s calls, %s%s\n", i+1,
			humanInt(p.TotalCalls), humanMs(p.TotalExecTimeMs), share)
		fmt.Fprintf(w, "    %d distinct orderings across %d fingerprint(s)\n",
			p.DistinctSorts, len(p.Variants))

		for _, v := range p.Variants {
			order := "(unsorted)"
			if len(v.SortClauses) > 0 {
				text, elided := truncate(strings.Join(v.SortClauses, " | "), width)
				order = text
				// Truncation can hide the tail column that distinguishes two
				// variants, rendering them as identical rows — tag the row so
				// they stay distinguishable.
				if elided && len(p.Variants) > 1 {
					order += "  " + shortFingerprint(v.Fingerprint)
				}
			}
			fmt.Fprintf(w, "      %12s  %10s calls  %s\n",
				humanMs(v.TotalExecTimeMs), humanInt(v.Calls), order)
		}
	}
	return nil
}

func humanMs(ms float64) string {
	switch {
	case ms >= 1000:
		return fmt.Sprintf("%.1fs", ms/1000)
	default:
		return fmt.Sprintf("%.0fms", ms)
	}
}

func humanInt(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	lead := len(s) % 3
	if lead > 0 {
		b.WriteString(s[:lead])
	}
	for i := lead; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func truncate(s string, n int) (string, bool) {
	s = strings.Join(strings.Fields(s), " ")
	r := []rune(s)
	if n <= 3 || len(r) <= n {
		return s, false
	}
	return string(r[:n-3]) + "...", true
}

// shortFingerprint gives enough of the hash to tell sibling variants apart on
// screen, without dumping the full fingerprint into every truncated row.
func shortFingerprint(fp string) string {
	const prefix = "sha1:"
	h := strings.TrimPrefix(fp, prefix)
	if len(h) > 8 {
		h = h[:8]
	}
	return prefix + h
}
