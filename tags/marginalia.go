package tags

import "strings"

func parseMarginalia(rawSQL string) []Tag {
	for _, body := range edgeBlockComments(rawSQL) {
		if tags := parseMarginaliaBody(body); len(tags) > 0 {
			return tags
		}
	}
	return nil
}

func parseMarginaliaBody(body string) []Tag {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}
	var out []Tag
	for pair := range strings.SplitSeq(body, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		sep := strings.IndexAny(pair, ":=")
		if sep <= 0 {
			return nil
		}
		key := strings.TrimSpace(pair[:sep])
		val := strings.TrimSpace(pair[sep+1:])
		if key == "" || !isIdent(key) {
			return nil
		}
		out = append(out, Tag{Key: key, Value: val, Format: FormatMarginalia})
	}
	return out
}

func isIdent(s string) bool {
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && (r >= '0' && r <= '9' || r == '.' || r == '-'):
		default:
			return false
		}
	}
	return true
}
