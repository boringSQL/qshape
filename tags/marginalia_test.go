package tags

import "testing"

// TestParseMarginalia_RailsRealistic mirrors the kind of comment Rails
// 7's `query_log_tags` emits when an ActiveRecord query fires inside a
// controller action. The format is leading block, `k:v` pairs. This is
// the dominant in-the-wild dialect for marginalia.
func TestParseMarginalia_RailsRealistic(t *testing.T) {
	sql := "/*application:billing,controller:orders,action:show*/ SELECT * FROM users WHERE id = $1"
	got := parseMarginalia(sql)
	if len(got) != 3 {
		t.Fatalf("expected 3 tags, got %d: %+v", len(got), got)
	}
	if v, _ := findTag(got, "application"); v != "billing" {
		t.Errorf("application = %q", v)
	}
	if v, _ := findTag(got, "controller"); v != "orders" {
		t.Errorf("controller = %q", v)
	}
}

// TestParseMarginalia_EqualsSeparator confirms the parser handles
// `k=v` in addition to `k:v`. Some libraries (older Rails plugins,
// custom Phoenix middleware) use `=` because it survives copy-paste
// into shells without quoting hassles.
func TestParseMarginalia_EqualsSeparator(t *testing.T) {
	sql := "/*application=billing,controller=orders*/ SELECT 1"
	got := parseMarginalia(sql)
	if v, _ := findTag(got, "application"); v != "billing" {
		t.Errorf("application = %q", v)
	}
	if v, _ := findTag(got, "controller"); v != "orders" {
		t.Errorf("controller = %q", v)
	}
}

// TestParseMarginalia_EOL covers the case where the marginalia block
// is emitted *after* the statement (Phoenix Ecto does this). The
// parser must accept both edges.
func TestParseMarginalia_EOL(t *testing.T) {
	sql := "SELECT 1 /*application:web,controller:users*/"
	got := parseMarginalia(sql)
	if v, _ := findTag(got, "controller"); v != "users" {
		t.Errorf("controller = %q", v)
	}
}

// TestParseMarginalia_MidStatementIgnored guards against parsing
// comments that aren't actually marginalia. A `/* comment */` in the
// middle of a SELECT could be a hint, a column annotation, or
// developer prose. Only SOL/EOL blocks are by convention tag
// carriers; anything mid-statement is intentionally skipped.
func TestParseMarginalia_MidStatementIgnored(t *testing.T) {
	sql := "SELECT id /*not:a:tag*/ FROM users"
	got := parseMarginalia(sql)
	if len(got) != 0 {
		t.Errorf("expected no tags from mid-statement comment, got %+v", got)
	}
}

// TestParseMarginalia_UnterminatedBlock locks in the "fail soft" rule
// from PLAN-TAGGING.md: malformed comments must not panic or error,
// they just produce no tags. pg_stat_statements occasionally returns
// truncated query text and we'd rather lose tags than crash capture.
func TestParseMarginalia_UnterminatedBlock(t *testing.T) {
	sql := "/*application:billing,controller:orders SELECT 1"
	got := parseMarginalia(sql)
	if len(got) > 0 {
		t.Errorf("expected nil from unterminated block, got %+v", got)
	}
}
