package qshape

import pg_query "github.com/pganalyze/pg_query_go/v6"

func Fingerprint(sql string) (string, error) {
	// Normalize first so ORM variants share a fingerprint
	canonical, err := Normalize(sql)
	if err != nil {
		return "", err
	}
	fp, err := pg_query.Fingerprint(canonical)
	if err != nil {
		return "", err
	}
	return "sha1:" + fp, nil
}
