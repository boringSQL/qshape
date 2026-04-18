package qshape

import pg_query "github.com/pganalyze/pg_query_go/v6"

func Fingerprint(sql string) (string, error) {
	fp, err := pg_query.Fingerprint(sql)
	if err != nil {
		return "", err
	}
	return "sha1:" + fp, nil
}
