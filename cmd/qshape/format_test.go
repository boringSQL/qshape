package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/boringsql/qshape"
)

func TestValidateSchemaVersionCurrent(t *testing.T) {
	doc := &clustersDoc{SchemaVersion: currentSchemaVersion}
	if err := validateSchemaVersion(doc); err != nil {
		t.Errorf("current version should validate: %v", err)
	}
}

func TestValidateSchemaVersionMissing(t *testing.T) {
	doc := &clustersDoc{}
	err := validateSchemaVersion(doc)
	if err == nil {
		t.Fatal("missing schema_version should error")
	}
	if !strings.Contains(err.Error(), "missing schema_version") {
		t.Errorf("error should mention missing schema_version, got: %v", err)
	}
}

func TestValidateSchemaVersionUnknown(t *testing.T) {
	doc := &clustersDoc{SchemaVersion: "99"}
	err := validateSchemaVersion(doc)
	if err == nil {
		t.Fatal("unknown schema_version should error")
	}
	if !strings.Contains(err.Error(), `"99"`) {
		t.Errorf("error should mention the rejected version, got: %v", err)
	}
}

func TestClustersDocRoundTrip(t *testing.T) {
	in := clustersDoc{
		SchemaVersion: currentSchemaVersion,
		Clusters: []qshape.Cluster{
			{Fingerprint: "sha1:abc", Canonical: "SELECT 1", TotalCalls: 5},
		},
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(in); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"schema_version":"1"`) {
		t.Errorf("encoded output missing schema_version:\n%s", buf.String())
	}
	var out clustersDoc
	if err := json.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if err := validateSchemaVersion(&out); err != nil {
		t.Errorf("round-trip output failed validation: %v", err)
	}
	if len(out.Clusters) != 1 || out.Clusters[0].Fingerprint != "sha1:abc" {
		t.Errorf("round-trip lost cluster data: %+v", out.Clusters)
	}
}
