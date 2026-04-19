package qshape

import pg_query "github.com/pganalyze/pg_query_go/v6"

func Fingerprint(sql string) (string, error) {
	// Normalize first so ORM variants share a fingerprint; fall back to raw
	// SQL when normalization hits a deparse gap so we still return a fingerprint
	target := sql
	if canonical, err := Normalize(sql); err == nil {
		target = canonical
	}
	fp, err := pg_query.Fingerprint(target)
	if err != nil {
		return "", err
	}
	return "sha1:" + fp, nil
}
