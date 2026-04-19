package qshape

import (
	"strings"
	"testing"
)

func TestFingerprintStable(t *testing.T) {
	fp1, err := Fingerprint("SELECT id FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	fp2, err := Fingerprint("SELECT id FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if fp1 != fp2 {
		t.Errorf("fingerprint not stable: %s vs %s", fp1, fp2)
	}
	if !strings.HasPrefix(fp1, "sha1:") {
		t.Errorf("expected sha1: prefix, got %q", fp1)
	}
}

func TestFingerprintIgnoresWhitespace(t *testing.T) {
	a, _ := Fingerprint("SELECT id FROM users WHERE id = 1")
	b, _ := Fingerprint("SELECT    id  FROM   users   WHERE  id=1")
	if a != b {
		t.Errorf("whitespace variation changed fingerprint: %s vs %s", a, b)
	}
}

func TestFingerprintIgnoresLiterals(t *testing.T) {
	a, _ := Fingerprint("SELECT id FROM users WHERE id = 1")
	b, _ := Fingerprint("SELECT id FROM users WHERE id = 42")
	if a != b {
		t.Errorf("literal variation changed fingerprint: %s vs %s", a, b)
	}
}

func TestFingerprintDistinguishesColumns(t *testing.T) {
	a, _ := Fingerprint("SELECT id FROM users WHERE id = 1")
	b, _ := Fingerprint("SELECT id FROM users WHERE name = 'x'")
	if a == b {
		t.Errorf("different predicates should fingerprint differently, both %s", a)
	}
}

func TestFingerprintInvalid(t *testing.T) {
	if _, err := Fingerprint("SELECT FROM WHERE"); err == nil {
		t.Error("expected error on invalid SQL")
	}
}
