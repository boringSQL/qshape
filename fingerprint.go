package qshape

import pg_query "github.com/pganalyze/pg_query_go/v6"

func Fingerprint(sql string) (string, error) {
	fp, _, err := fingerprintNormalized(sql)
	return fp, err
}

// fingerprintNormalized returns the fingerprint together with the text it was computed
// from, so setCanonical does not re-derive it with a second Parse+Deparse.
func fingerprintNormalized(sql string) (fingerprint, canonical string, err error) {
	// Normalize first so ORM variants share a fingerprint; fall back to raw
	// SQL when normalization hits a deparse gap so we still return a fingerprint
	canonical = sql
	if normalized, nerr := Normalize(sql); nerr == nil {
		canonical = normalized
	}
	fp, err := pg_query.Fingerprint(canonical)
	if err != nil {
		return "", "", err
	}
	return "sha1:" + fp, canonical, nil
}
