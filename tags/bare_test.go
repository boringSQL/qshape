package tags

import "testing"

// TestParseBareComment_Hibernate represents the canonical Hibernate
// `use_sql_comments=true` output: the JPQL/HQL source statement
// dropped verbatim as a leading block comment. The whole body
// becomes a sanitized slug — ideal as a regresql-stub filename.
func TestParseBareComment_Hibernate(t *testing.T) {
	sql := "/* select u from User u where u.id = ?1 */ select user0_.id from users user0_ where user0_.id = $1"
	got := parseBareComment(sql)
	if len(got) != 1 {
		t.Fatalf("expected 1 tag, got %d", len(got))
	}
	if got[0].Key != "name" {
		t.Errorf("key = %q, want name", got[0].Key)
	}
	want := "select-u-from-user-u-where-u-id-1"
	if got[0].Value != want {
		t.Errorf("name = %q, want %q", got[0].Value, want)
	}
}

// TestParseBareComment_LineForm covers the `-- some label` variant
// where there's no key-value structure. It's a bare label, not a
// header (no `:`). Must round-trip into a `name` tag.
func TestParseBareComment_LineForm(t *testing.T) {
	sql := "-- daily cleanup job\nDELETE FROM sessions WHERE expires_at < now()"
	got := parseBareComment(sql)
	if len(got) != 1 || got[0].Value != "daily-cleanup-job" {
		t.Errorf("got %+v, want name=daily-cleanup-job", got)
	}
}

// TestSanitizeName locks in the slug rules verbatim — these are
// also re-applied by the regresql-stub generator as defense in
// depth, so any change here cascades downstream. The rules:
// lowercase, non-[a-z0-9-] → '-', collapse runs of '-', trim
// leading/trailing '-', truncate to 80.
func TestSanitizeName(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"GetUserByID", "getuserbyid"},
		{"foo bar baz", "foo-bar-baz"},
		{"  -trim--me-  ", "trim-me"},
		{"emoji😀ok", "emoji-ok"},
		{"", ""},
	}
	for _, c := range cases {
		if got := SanitizeName(c.in); got != c.want {
			t.Errorf("SanitizeName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestSanitizeName_Truncation makes sure the 80-char cap is enforced
// even when the input is a several-hundred-character paragraph (a
// real risk when bare comments contain entire JPQL queries).
func TestSanitizeName_Truncation(t *testing.T) {
	in := make([]byte, 200)
	for i := range in {
		in[i] = 'a'
	}
	got := SanitizeName(string(in))
	if len(got) > 80 {
		t.Errorf("len = %d, want ≤ 80", len(got))
	}
}

// TestExtract_DispatchesByFormat is the end-to-end happy path: feed
// SQL in one of each format and confirm the right tags pop out.
// This locks in the wiring inside Extract — if the dispatch switch
// breaks, every downstream consumer breaks too.
func TestExtract_DispatchesByFormat(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		key  string
		val  string
	}{
		{"header", "-- name: my-query\nSELECT 1", "name", "my-query"},
		{"sqlcommenter", "SELECT 1 /*controller='X'*/", "controller", "X"},
		{"marginalia", "/*application:billing*/ SELECT 1", "application", "billing"},
		{"bare", "/* Free Form Name */ SELECT 1", "name", "free-form-name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Extract(c.sql)
			if v, _ := findTag(got, c.key); v != c.val {
				t.Errorf("%s: got %q for key %s, want %q", c.name, v, c.key, c.val)
			}
		})
	}
}

// TestExtract_Untagged: the fast path. Vanilla SQL must return nil
// so callers can branch cheaply. This is the most common case in
// real workloads — most queries carry no tags.
func TestExtract_Untagged(t *testing.T) {
	if got := Extract("SELECT 1 FROM users"); got != nil {
		t.Errorf("untagged SQL should return nil, got %+v", got)
	}
}
