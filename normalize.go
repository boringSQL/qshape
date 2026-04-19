package qshape

import pg_query "github.com/pganalyze/pg_query_go/v6"

func Normalize(sql string) (string, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return "", err
	}
	if err := reshape(tree); err != nil {
		return "", err
	}
	return pg_query.Deparse(tree)
}
