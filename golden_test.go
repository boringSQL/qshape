package qshape

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/boringsql/qshape/tags"
)

// -update regenerates testdata/fingerprints.json. The committed file predates the
// parallel fast path and pins the values dryrun's content hash is computed from;
// regenerating it changes every user's hashes.
var updateGolden = flag.Bool("update", false, "regenerate testdata/fingerprints.json")

// Seeds only, never the captured corpus: clusters.json holds real customer SQL and is
// gitignored.
type goldenEntry struct {
	Raw         string `json:"raw"`
	Fingerprint string `json:"fingerprint"`
	Canonical   string `json:"canonical"`
}

const goldenPath = "testdata/fingerprints.json"

func goldenFor(t *testing.T, sql string) goldenEntry {
	t.Helper()
	clusters, err := GroupWithPolicy([]Query{{Raw: sql, Calls: 1}}, tags.DefaultPolicy())
	if err != nil {
		t.Fatalf("GroupWithPolicy(%.60q): %v", sql, err)
	}
	if len(clusters) != 1 {
		t.Fatalf("GroupWithPolicy(%.60q): got %d clusters, want 1", sql, len(clusters))
	}
	return goldenEntry{Raw: sql, Fingerprint: clusters[0].Fingerprint, Canonical: clusters[0].Canonical}
}

func TestGroupGolden(t *testing.T) {
	if *updateGolden {
		entries := make([]goldenEntry, 0, len(prettifySeeds))
		for _, s := range prettifySeeds {
			entries = append(entries, goldenFor(t, s))
		}
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		blob, err := json.MarshalIndent(entries, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, append(blob, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s (%d entries)", goldenPath, len(entries))
		return
	}

	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden: %v (regenerate with `go test -run TestGroupGolden -update`)", err)
	}
	var want []goldenEntry
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("golden file is empty")
	}

	covered := make(map[string]bool, len(want))
	for _, w := range want {
		covered[w.Raw] = true
		got := goldenFor(t, w.Raw)
		if got.Fingerprint != w.Fingerprint {
			t.Errorf("fingerprint changed for %.60q:\n got=%q\nwant=%q", w.Raw, got.Fingerprint, w.Fingerprint)
		}
		if got.Canonical != w.Canonical {
			t.Errorf("canonical changed for %.60q:\n got=%q\nwant=%q", w.Raw, got.Canonical, w.Canonical)
		}
	}
	// Seeds added after the fixture was generated must not go silently uncovered.
	for _, s := range prettifySeeds {
		if !covered[s] {
			t.Errorf("seed not in the golden file: %.60q\n(regenerate with `go test -run TestGroupGolden -update`)", s)
		}
	}
}
