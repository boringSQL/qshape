package tags

import "testing"

// findTag scans a []Tag slice for a given key — convenience for tests
// since the parser preserves order but we don't want to assert exact
// positions (the parser can interleave synthetic tags like
// `cardinality` near `name`, and we don't want to lock that down).
func findTag(tags []Tag, key string) (string, bool) {
	for _, t := range tags {
		if t.Key == key {
			return t.Value, true
		}
	}
	return "", false
}

// TestParseHeader_BoringSQLStyle covers the canonical sqlc /
// boringSQL-queries shape — multiple `-- key: value` lines followed by
// the SQL body. Confirms the parser:
//   - returns every header line, not just `name`
//   - stops at the first non-`--` line (the SQL must not get parsed)
//   - preserves arbitrary keys alongside reserved ones
func TestParseHeader_BoringSQLStyle(t *testing.T) {
	sql := `-- name: get-user-by-email
-- description: Retrieve user by email efficiently
-- max-cost: 100
SELECT * FROM users WHERE email = $1;`
	got := parseHeader(sql)
	if len(got) != 3 {
		t.Fatalf("expected 3 tags, got %d: %+v", len(got), got)
	}
	if v, _ := findTag(got, "name"); v != "get-user-by-email" {
		t.Errorf("name = %q, want get-user-by-email", v)
	}
	if v, _ := findTag(got, "description"); v != "Retrieve user by email efficiently" {
		t.Errorf("description = %q", v)
	}
	if v, _ := findTag(got, "max-cost"); v != "100" {
		t.Errorf("max-cost = %q", v)
	}
}

// TestParseHeader_SqlcCardinality locks in the sqlc compatibility
// behavior: the `:one|:many|:exec|…` suffix on the `name` value is
// stripped off and re-emitted as a separate synthetic `cardinality`
// tag. Without this, sqlc-style names like `GetUserByID :one` would
// pollute regresql stub filenames with a literal `-one` suffix.
func TestParseHeader_SqlcCardinality(t *testing.T) {
	for _, card := range []string{"one", "many", "exec", "execrows", "batchexec", "batchmany", "batchone", "copyfrom"} {
		t.Run(card, func(t *testing.T) {
			sql := "-- name: GetUserByID :" + card + "\nSELECT 1"
			got := parseHeader(sql)
			if v, _ := findTag(got, "name"); v != "GetUserByID" {
				t.Errorf("name = %q, want GetUserByID (suffix should be stripped)", v)
			}
			if v, _ := findTag(got, "cardinality"); v != card {
				t.Errorf("cardinality = %q, want %s", v, card)
			}
		})
	}
}

// TestParseHeader_LeadingWhitespaceAndCRLF guards against the real
// case where SQL comes out of pg_stat_statements with Windows line
// endings or extra indentation. The parser must be tolerant — failing
// on whitespace is a recurring papercut in tag harvesters.
func TestParseHeader_LeadingWhitespaceAndCRLF(t *testing.T) {
	sql := "   -- name: foo\r\n  -- description: bar\r\nSELECT 1"
	got := parseHeader(sql)
	if v, _ := findTag(got, "name"); v != "foo" {
		t.Errorf("name = %q, want foo", v)
	}
	if v, _ := findTag(got, "description"); v != "bar" {
		t.Errorf("description = %q, want bar", v)
	}
}

// TestParseHeader_StopsAtFirstNonComment verifies the parser doesn't
// keep scanning past the SQL body. Header tags are leading-only by
// convention; tags inside or after the statement belong to the other
// formats. If this test fails, the parser is over-greedy and will
// misclassify SQL string literals containing `--` as headers.
func TestParseHeader_StopsAtFirstNonComment(t *testing.T) {
	sql := `-- name: real
SELECT 1
-- name: fake`
	got := parseHeader(sql)
	if len(got) != 1 {
		t.Fatalf("expected 1 tag, got %d", len(got))
	}
	if got[0].Value != "real" {
		t.Errorf("got %q, want real", got[0].Value)
	}
}
