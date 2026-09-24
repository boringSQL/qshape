package qshape

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// WalkNodes visits n and every Node reachable from it, covering all
// pg_query node types via protobuf reflection. forEachChild stays for
// walks that must remain selective (stripSortClause).
func WalkNodes(n *pg_query.Node, visit func(*pg_query.Node)) {
	if n == nil {
		return
	}
	walkNodeMessage(n.ProtoReflect(), visit)
}

func walkNodeMessage(m protoreflect.Message, visit func(*pg_query.Node)) {
	if node, ok := m.Interface().(*pg_query.Node); ok {
		visit(node)
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind:
			return true
		case fd.IsList():
			list := v.List()
			for i := 0; i < list.Len(); i++ {
				walkNodeMessage(list.Get(i).Message(), visit)
			}
		default:
			walkNodeMessage(v.Message(), visit)
		}
		return true
	})
}
