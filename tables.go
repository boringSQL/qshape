package qshape

import (
	"sort"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TableRef is one relation a statement references.
//
// Schema is empty when the SQL did not qualify the name, which is the common case and
// is NOT resolvable here: pg_stat_statements carries no search_path, so `FROM orders`
// in a multi-tenant database may mean a different relation per session. Callers must
// treat an unqualified name as a name to match, not as an identity.
type TableRef struct {
	Schema string `json:"schema,omitempty"`
	Name   string `json:"name"`
}

// Tables reports the relations a query references, deduplicated and ordered
// (schema, name). It is a read-only walk over the parse tree: it does not reshape,
// normalize or fingerprint, so it cannot move a Fingerprint value and needs no
// GroupingVersion bump.
//
// Every RangeVar in the tree counts, which is what makes the DML targets come out
// right: `INSERT INTO orders`, `UPDATE orders`, `DELETE FROM orders` and `MERGE INTO
// orders` hang the target off the statement's own Relation field, not off a FROM list,
// and a walker written around FROM clauses reports zero tables for all four.
//
// Scoped to DML (SELECT/INSERT/UPDATE/DELETE/MERGE/VALUES), which is what
// pg_stat_statements corpora carry. Utility statements are best-effort and draw no
// line between a relation used and one defined or dropped: `CREATE TABLE t` reports t,
// while `DROP TABLE t` reports nothing at all (DropStmt names its target as a string
// list, not a RangeVar).
//
// CTE names are excluded when unqualified: `WITH recent AS (...) SELECT * FROM recent`
// references one real table, not two. The exclusion is by name across the whole input
// rather than per lexical scope, so a real table sharing a name with a CTE declared in
// any other scope (or, for multi-statement input, in any other statement) is dropped
// too. That is the deliberate direction to be wrong in: a missing cross-link is a gap,
// an invented one is a false claim about what a query touches.
//
// An empty result is legitimate and does NOT mean the query was unreadable:
// `SELECT $1`, `VALUES ($1)` and a blank string all reference no relation. Callers that
// need "could we read this at all" must use the error, not the length.
func Tables(sql string) ([]TableRef, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}

	var refs []TableRef
	ctes := map[string]bool{}
	walkMessages(tree.ProtoReflect(), func(m protoreflect.Message) {
		switch msg := m.Interface().(type) {
		case *pg_query.RangeVar:
			if msg.Relname != "" {
				refs = append(refs, TableRef{Schema: msg.Schemaname, Name: msg.Relname})
			}
		case *pg_query.CommonTableExpr:
			if msg.Ctename != "" {
				ctes[msg.Ctename] = true
			}
		}
	})

	seen := make(map[TableRef]bool, len(refs))
	out := make([]TableRef, 0, len(refs))
	for _, r := range refs {
		if r.Schema == "" && ctes[r.Name] {
			continue
		}
		if seen[r] {
			continue
		}
		seen[r] = true
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// walkMessages visits every message in a parse tree, generically.
//
// Deliberately reflective rather than a type switch over node kinds: relations hide in
// places a hand-written walker forgets (a MERGE's source relation, an ON CONFLICT
// target, a LATERAL function's arguments, a set operation's arms), and each forgotten
// case is a query that silently reports no tables. Range visits only populated fields,
// so unset messages cost nothing.
//
// Range's iteration order is UNDEFINED. Tables is deterministic only because it
// collects CTE names in a full pass before filtering, and sorts at the end; fusing the
// filter into the walk, or dropping the sort because the walk "looks ordered", makes
// the output depend on map order and the tests pass most of the time.
func walkMessages(m protoreflect.Message, visit func(protoreflect.Message)) {
	visit(m)
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					walkMessages(mv.Message(), visit)
					return true
				})
			}
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind {
				l := v.List()
				for i := 0; i < l.Len(); i++ {
					walkMessages(l.Get(i).Message(), visit)
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			walkMessages(v.Message(), visit)
		}
		return true
	})
}
