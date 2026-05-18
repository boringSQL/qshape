package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boringsql/qshape"
)

func TestRewriteParams(t *testing.T) {
	got, names := rewriteParams("SELECT id FROM users WHERE id = $1 AND tenant_id = $2 AND id = $1")
	want := "SELECT id FROM users WHERE id = :param1 AND tenant_id = :param2 AND id = :param1"
	if got != want {
		t.Errorf("rewrite mismatch:\n got: %s\nwant: %s", got, want)
	}
	if len(names) != 2 || names[0] != "param1" || names[1] != "param2" {
		t.Errorf("unexpected names: %v", names)
	}
}

func TestSampleValuesForParams(t *testing.T) {
	fixJSON := `{
      "tables": {
        "auth.user_account": {
          "columns": ["user_id", "email"],
          "rows": [[42, "a@b.co"], [99, "x@y.co"], [null, "z@z.co"]]
        }
      }
    }`
	var fix fixtureDoc
	if err := json.Unmarshal([]byte(fixJSON), &fix); err != nil {
		t.Fatal(err)
	}
	attrs := []qshape.ParamAttribution{
		{Position: 1, Schema: "auth", Table: "user_account", Column: "user_id", Confidence: "exact"},
	}
	out := sampleValuesForParams([]string{"param1"}, attrs, &fix, 3)
	if len(out["param1"]) != 2 {
		t.Errorf("expected 2 non-null values, got %v", out["param1"])
	}
	if out["param1"][0].(float64) != 42 {
		t.Errorf("first value = %v, want 42", out["param1"][0])
	}
}

func TestSampleValuesSkipsUnattributed(t *testing.T) {
	fix := &fixtureDoc{}
	attrs := []qshape.ParamAttribution{
		{Position: 1, Confidence: "none"}, // no table/column
	}
	out := sampleValuesForParams([]string{"param1"}, attrs, fix, 2)
	if len(out) != 0 {
		t.Errorf("expected no samples, got %+v", out)
	}
}

func TestYAMLScalarStringEscaping(t *testing.T) {
	cases := map[any]string{
		"hello":   `"hello"`,
		`a"b`:     `"a\"b"`,
		int64(5): `5`,
		nil:      `~`,
		true:     `true`,
		3.14:     `3.14`,
	}
	for in, want := range cases {
		if got := yamlScalar(in); got != want {
			t.Errorf("yamlScalar(%v) = %q, want %q", in, got, want)
		}
	}
}

// TestStubSlugFor_PrefersTagName: when RegresqlMeta["name"] is set
// on the cluster, the stub filename uses that name (re-sanitized)
// instead of the q##-<fp8> fallback. This is the cosmetic win that
// makes a regresql-stub dump usable for humans.
func TestStubSlugFor_PrefersTagName(t *testing.T) {
	c := qshape.Cluster{
		Fingerprint:  "sha1:abc12345",
		RegresqlMeta: map[string]string{"name": "GetUserByEmail"},
	}
	got := stubSlugFor(1, c)
	if got != "getuserbyemail" {
		t.Errorf("slug = %q, want getuserbyemail", got)
	}
}

// TestStubSlugFor_FallsBackToFingerprint: when no name tag is
// present, the slug falls back to the existing q##-<fp8> shape.
// This guards untagged corpuses against accidental regression — the
// pre-tagging behavior must still work.
func TestStubSlugFor_FallsBackToFingerprint(t *testing.T) {
	c := qshape.Cluster{Fingerprint: "sha1:abc12345"}
	got := stubSlugFor(7, c)
	if got != "q07-abc12345" {
		t.Errorf("slug = %q, want q07-abc12345", got)
	}
}

// TestWriteSQLStub_EmitsTagsAndMeta is the golden-ish content test:
// for a fully-tagged cluster we verify the produced .sql file
// header contains description, max-cost, sorted tag-* lines, and
// the slug. Sorted tag lines matter because map iteration is random
// and we don't want diffs to flap.
func TestWriteSQLStub_EmitsTagsAndMeta(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stub.sql")
	c := qshape.Cluster{
		Fingerprint: "sha1:abc12345",
		TotalCalls:  100,
		Members:     []qshape.Query{{}},
		Owners: map[string]string{
			"controller":  "OrdersController",
			"application": "billing",
		},
		RegresqlMeta: map[string]string{
			"description": "Order lookup by id",
			"max-cost":    "100",
		},
	}
	if err := writeSQLStub(path, "lookup-order", c, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	mustContain(t, out, "-- name: lookup-order")
	mustContain(t, out, "-- description: Order lookup by id")
	mustContain(t, out, "-- max-cost: 100")
	mustContain(t, out, "-- tag-application: billing")
	mustContain(t, out, "-- tag-controller: OrdersController")
	// Sort check: 'application' must appear before 'controller'.
	if strings.Index(out, "tag-application") > strings.Index(out, "tag-controller") {
		t.Errorf("tag lines should be lex-sorted by key, got:\n%s", out)
	}
}

func mustContain(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Errorf("output missing %q. full:\n%s", sub, s)
	}
}

// TestStubSlugFor_SynthesizesFromOwners: marginalia / sqlcommenter
// workloads (Rails, Datadog) don't typically carry a `name` tag —
// they carry application / controller / action. Rather than fall
// back to q##-<fp8> for the dominant case, the slug is synthesized
// by joining those three (sanitized) so a stub becomes e.g.
// `billing-orderscontroller-show.sql`. Job-only tags also work
// (background workers) when no HTTP triple is present.
func TestStubSlugFor_SynthesizesFromOwners(t *testing.T) {
	cases := []struct {
		name   string
		owners map[string]string
		want   string
	}{
		{
			"controller+action",
			map[string]string{"controller": "OrdersController", "action": "show"},
			"orderscontroller-show",
		},
		{
			"application+controller+action",
			map[string]string{"application": "billing", "controller": "Orders", "action": "show"},
			"billing-orders-show",
		},
		{
			"job-only",
			map[string]string{"job": "EmailDigestJob"},
			"emaildigestjob",
		},
		{
			"job-ignored-when-http-present",
			map[string]string{"controller": "X", "job": "ignored"},
			"x",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cl := qshape.Cluster{Fingerprint: "sha1:abc12345", Owners: c.owners}
			if got := stubSlugFor(1, cl); got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

// TestStubSlugFor_NameWinsOverSynthetic: explicit RegresqlMeta["name"]
// is always preferred over a synthetic owner-derived slug. This is
// the ordering documented in stubSlugFor — explicit > synthetic >
// fallback.
func TestStubSlugFor_NameWinsOverSynthetic(t *testing.T) {
	c := qshape.Cluster{
		Fingerprint:  "sha1:abc12345",
		RegresqlMeta: map[string]string{"name": "MyExplicitName"},
		Owners:       map[string]string{"controller": "Other"},
	}
	if got := stubSlugFor(1, c); got != "myexplicitname" {
		t.Errorf("got %q, want myexplicitname", got)
	}
}
