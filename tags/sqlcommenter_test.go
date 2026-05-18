package tags

import "testing"

// TestParseSqlcommenter_Datadog represents the typical Datadog APM
// trace injection: trailing block, single-quoted values, includes
// trace correlation IDs alongside controller/action metadata.
// Critically — the parser must produce a `traceparent` tag here so
// the classifier downstream can recognize it as dynamic and drop the
// value (the value would be high-cardinality and noisy in git).
func TestParseSqlcommenter_Datadog(t *testing.T) {
	sql := "SELECT * FROM users WHERE id = $1 /*controller='OrdersController',action='show',traceparent='00-abc123-def-01'*/"
	got := parseSqlcommenter(sql)
	if v, _ := findTag(got, "controller"); v != "OrdersController" {
		t.Errorf("controller = %q", v)
	}
	if v, _ := findTag(got, "action"); v != "show" {
		t.Errorf("action = %q", v)
	}
	if v, _ := findTag(got, "traceparent"); v != "00-abc123-def-01" {
		t.Errorf("traceparent = %q", v)
	}
}

// TestParseSqlcommenter_PercentDecoded confirms the parser runs
// stdlib `net/url.QueryUnescape` on values. The sqlcommenter spec
// requires percent-encoding for any character that could break the
// `key='value'` shape — particularly `'`, `,`, and `*/`. If decoding
// is skipped, downstream consumers see encoded gibberish.
func TestParseSqlcommenter_PercentDecoded(t *testing.T) {
	sql := "SELECT 1 /*controller='Orders%20Controller'*/"
	got := parseSqlcommenter(sql)
	if v, _ := findTag(got, "controller"); v != "Orders Controller" {
		t.Errorf("controller = %q, want 'Orders Controller'", v)
	}
}

// TestParseSqlcommenter_EscapedQuote covers the `\'` escape sequence
// used when a value legitimately contains a single quote. The parser
// must unwrap `\'` to `'` inside the value rather than treating it as
// a delimiter (which would truncate the value).
func TestParseSqlcommenter_EscapedQuote(t *testing.T) {
	sql := `SELECT 1 /*action='it\'s-show-time'*/`
	got := parseSqlcommenter(sql)
	if v, _ := findTag(got, "action"); v != "it's-show-time" {
		t.Errorf("action = %q, want it's-show-time", v)
	}
}

// TestParseSqlcommenter_EmbeddedStarSlash covers the spec's
// "injection-safe" requirement: a raw `*/` inside a value would
// terminate the SQL comment early. The spec mandates encoding it as
// `%2A%2F`. If a buggy writer emits raw `*/`, the parser must
// terminate at that point and discard the remainder rather than
// panic or read past the comment boundary.
func TestParseSqlcommenter_EmbeddedStarSlash(t *testing.T) {
	sql := "SELECT 1 /*controller='good',action='bad*/"
	got := parseSqlcommenter(sql)
	if v, _ := findTag(got, "controller"); v != "good" {
		t.Errorf("controller = %q", v)
	}
	if _, ok := findTag(got, "action"); ok {
		t.Errorf("action should be discarded on malformed input")
	}
}

// TestParseSqlcommenter_MalformedMissingQuote ensures the parser
// returns soft-failure (whatever pairs it managed to read) on
// missing closing quotes, not a panic. Real-world tag writers can
// truncate output mid-statement and we have to survive it.
func TestParseSqlcommenter_MalformedMissingQuote(t *testing.T) {
	sql := "SELECT 1 /*controller='ok',action='unclosed*/"
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("parser panicked on malformed input: %v", r)
		}
	}()
	_ = parseSqlcommenter(sql)
}

// TestParseSqlcommenter_EmptyValue confirms `key=''` produces a tag
// with empty value rather than being dropped. Whether downstream
// chooses to keep or skip empty values is the classifier's call;
// the parser preserves what's there.
func TestParseSqlcommenter_EmptyValue(t *testing.T) {
	sql := "SELECT 1 /*controller=''*/"
	got := parseSqlcommenter(sql)
	v, ok := findTag(got, "controller")
	if !ok {
		t.Fatal("expected controller tag")
	}
	if v != "" {
		t.Errorf("controller = %q, want empty", v)
	}
}

// TestParseSqlcommenter_RepeatedKey: per spec, lex-sort and unique
// keys are the writer's responsibility. The parser doesn't dedupe —
// callers downstream are expected to handle it (the Classify pass
// puts a single value into the Owners map, last-wins). This test
// just locks in that we don't silently drop one.
func TestParseSqlcommenter_RepeatedKey(t *testing.T) {
	sql := "SELECT 1 /*controller='A',controller='B'*/"
	got := parseSqlcommenter(sql)
	count := 0
	for _, tg := range got {
		if tg.Key == "controller" {
			count++
		}
	}
	if count != 2 {
		t.Errorf("expected 2 controller tags from parser, got %d", count)
	}
}
