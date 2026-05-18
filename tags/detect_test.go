package tags

import "testing"

// TestDetectFormat is the single source of truth for "given some raw
// SQL, does DetectFormat point at the right parser?". Each case
// represents one of the four supported tag conventions seen in the
// wild (custom header, sqlcommenter, marginalia, bare-comment) plus a
// negative case (untagged SQL). If you add a fifth format, add a row
// here AND extend the dispatch switch in Extract.
func TestDetectFormat(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want Format
		ok   bool
	}{
		// sqlc / boringSQL-queries: a leading `-- key: value` line.
		// Detection only needs the first such line to fire.
		{"header", "-- name: get-user\nSELECT 1", FormatHeader, true},

		// sqlcommenter (Datadog/NewRelic/OTel): trailing block with
		// single-quoted, comma-separated key='value' pairs.
		{"sqlcommenter", "SELECT 1 /*controller='X'*/", FormatSqlcommenter, true},

		// Marginalia (Rails ActiveRecord, Phoenix Ecto): leading or
		// trailing block with `k:v` separators, no quoting.
		{"marginalia-leading-colon", "/*application:billing*/ SELECT 1", FormatMarginalia, true},

		// Marginalia again, this time with `=` and at end-of-line.
		// Same format — the parser handles both punctuation choices.
		{"marginalia-trailing-eq", "SELECT 1 /*application=billing*/", FormatMarginalia, true},

		// Bare comment (Hibernate `use_sql_comments=true`, dbt,
		// PostgREST, hand-written DBA scripts): no structure, just a
		// human-readable label. Sanitized into a slug downstream.
		{"bare-block", "/* GetUserByID */ SELECT 1", FormatBareComment, true},

		// A `--` line variant of bare-comment: free-form text with no
		// `:` separator (otherwise it'd be a header).
		{"bare-line", "-- some free form comment\nSELECT 1", FormatBareComment, true},

		// Negative case: vanilla SQL with no comments at all. Must
		// return (0, false) so callers can skip the parse entirely.
		{"none", "SELECT 1", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := DetectFormat(c.sql)
			if ok != c.ok || got != c.want {
				t.Errorf("DetectFormat = (%v, %v), want (%v, %v)", got, ok, c.want, c.ok)
			}
		})
	}
}

// TestDetectStopsAtFirstMatch locks in the precedence order documented
// in PLAN-TAGGING.md §6.9: header → sqlcommenter → marginalia → bare.
// A statement carrying both a header AND a sqlcommenter trailer must
// resolve to header, because mixing formats in one statement is not
// supported and "first wins" is the only stable rule. If a future
// change reorders detection, this test fails loudly.
func TestDetectStopsAtFirstMatch(t *testing.T) {
	sql := "-- name: x\nSELECT 1 /*controller='Y'*/"
	got, ok := DetectFormat(sql)
	if !ok || got != FormatHeader {
		t.Errorf("header should win over sqlcommenter, got %v %v", got, ok)
	}
}
