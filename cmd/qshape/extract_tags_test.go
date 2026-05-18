package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestRunExtractTags_Sqlcommenter: pipe a sqlcommenter-tagged SQL
// through extract-tags, get classified JSON out. This is the
// shell-pipeline use case: `cat auto_explain.log | qshape extract-tags | jq`.
// Locks in the public CLI shape: stdin → stdout JSON with the same
// owners/regresql_meta/dynamic_keys fields as clusters.json.
func TestRunExtractTags_Sqlcommenter(t *testing.T) {
	in := strings.NewReader("SELECT 1 /*controller='X',traceparent='abc'*/")
	var out bytes.Buffer
	if err := runExtractTags("", in, &out); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Owners      map[string]string `json:"Owners"`
		DynamicKeys []struct {
			Key string `json:"Key"`
		} `json:"DynamicKeys"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v\noutput:\n%s", err, out.String())
	}
	if got.Owners["controller"] != "X" {
		t.Errorf("Owners[controller] = %q, want X", got.Owners["controller"])
	}
	foundTrace := false
	for _, d := range got.DynamicKeys {
		if d.Key == "traceparent" {
			foundTrace = true
		}
	}
	if !foundTrace {
		t.Errorf("expected traceparent in DynamicKeys, got %+v", got.DynamicKeys)
	}
}

// TestRunExtractTags_Untagged: untagged SQL produces an empty
// ClassifiedTags object, not an error. Important for piping: the
// command should never fail just because no tags exist.
func TestRunExtractTags_Untagged(t *testing.T) {
	in := strings.NewReader("SELECT 1 FROM users")
	var out bytes.Buffer
	if err := runExtractTags("", in, &out); err != nil {
		t.Fatal(err)
	}
}
