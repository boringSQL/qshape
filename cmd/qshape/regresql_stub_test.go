package main

import (
	"encoding/json"
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
