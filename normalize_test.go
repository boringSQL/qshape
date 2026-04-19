package qshape

import (
	"strings"
	"testing"
)

func TestNormalize(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"plain select", "SELECT id FROM users"},
		{"select with AS", "SELECT id AS x FROM users"},
		{"aliased table", "SELECT u.id FROM users u WHERE u.id = $1"},
		{"where with literal", "SELECT id FROM users WHERE id = 42"},
		{"multi-column", "SELECT id, name, email FROM users WHERE id = $1"},
		{"insert", "INSERT INTO users (id, name) VALUES ($1, $2)"},
		{"update", "UPDATE users SET name = $1 WHERE id = $2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Normalize(tc.in)
			if err != nil {
				t.Fatalf("Normalize error: %v", err)
			}
			if strings.TrimSpace(got) == "" {
				t.Fatalf("Normalize returned empty output for %q", tc.in)
			}
			again, err := Normalize(got)
			if err != nil {
				t.Fatalf("idempotence Normalize error: %v", err)
			}
			if again != got {
				t.Errorf("not idempotent:\n  first:  %q\n  second: %q", got, again)
			}
		})
	}
}

func TestNormalizeInvalid(t *testing.T) {
	if _, err := Normalize("SELECT FROM WHERE"); err == nil {
		t.Error("expected error on invalid SQL")
	}
}
