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

// Golden ORM fixtures: each group's variants must normalize to one canonical
// form (decorative aliases stripped, optional AS absorbed by the deparser,
// AND-predicates sorted), and distinct groups must not collide.
func TestNormalizeORMFixtures(t *testing.T) {
	groups := [][]string{
		// Rails .where(id: ...) — alias, AS, renamed-alias variants
		{
			"SELECT id, name FROM users WHERE id = $1",
			"SELECT u.id, u.name FROM users u WHERE u.id = $1",
			"SELECT u.id, u.name FROM users AS u WHERE u.id = $1",
			"SELECT users_alias.id, users_alias.name FROM users users_alias WHERE users_alias.id = $1",
		},
		// Rails/ActiveRecord .joins(:posts) — aliased JOIN + AND reorder
		{
			"SELECT u.id, p.title FROM users u JOIN posts p ON p.user_id = u.id WHERE u.id = $1 AND p.published = $2",
			"SELECT u.id, p.title FROM users u JOIN posts p ON p.user_id = u.id WHERE p.published = $2 AND u.id = $1",
			"SELECT u.id, p.title FROM users AS u INNER JOIN posts AS p ON p.user_id = u.id WHERE u.id = $1 AND p.published = $2",
		},
		// Soft-delete scope with AND reorder
		{
			"SELECT id FROM users WHERE tenant_id = $1 AND deleted_at IS NULL",
			"SELECT u.id FROM users u WHERE u.tenant_id = $1 AND u.deleted_at IS NULL",
			"SELECT u.id FROM users AS u WHERE u.deleted_at IS NULL AND u.tenant_id = $1",
		},
		// UPDATE with AND reorder
		{
			"UPDATE users SET name = $1 WHERE id = $2 AND tenant_id = $3",
			"UPDATE users SET name = $1 WHERE tenant_id = $3 AND id = $2",
		},
		// DELETE with alias + AND reorder
		{
			"DELETE FROM users WHERE id = $1 AND tenant_id = $2",
			"DELETE FROM users u WHERE u.id = $1 AND u.tenant_id = $2",
			"DELETE FROM users WHERE tenant_id = $2 AND id = $1",
		},
		// Aggregate with alias + AND reorder (Prisma/Sequelize count pattern)
		{
			"SELECT COUNT(*) FROM orders WHERE status = $1 AND user_id = $2",
			"SELECT COUNT(*) FROM orders o WHERE o.status = $1 AND o.user_id = $2",
			"SELECT COUNT(*) FROM orders WHERE user_id = $2 AND status = $1",
		},
		// INSERT with RETURNING — separate canonical form from plain INSERT
		{
			"INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
		},
		// Prisma findUnique-style alias variants
		{
			"SELECT id, email, created_at FROM users WHERE email = $1 LIMIT 1",
			"SELECT u.id, u.email, u.created_at FROM users u WHERE u.email = $1 LIMIT 1",
		},
	}

	canonicals := make(map[string]int)
	total := 0
	for gi, group := range groups {
		var first string
		for vi, raw := range group {
			got, err := Normalize(raw)
			if err != nil {
				t.Fatalf("group %d variant %d: Normalize(%q) error: %v", gi, vi, raw, err)
			}
			if vi == 0 {
				first = got
				if prev, dup := canonicals[got]; dup {
					t.Errorf("group %d canonical %q collides with group %d", gi, got, prev)
				}
				canonicals[got] = gi
			} else if got != first {
				t.Errorf("group %d variant %d diverged:\n  first:  %q\n  this:   %q\n  input:  %q",
					gi, vi, first, got, raw)
			}
			total++
		}
	}
	if total < 20 {
		t.Errorf("fixture count = %d, want >= 20 per Phase 0.x target", total)
	}
}
