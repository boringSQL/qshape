package tags

import (
	"net/url"
	"strings"
)

func parseSqlcommenter(rawSQL string) []Tag {
	s := strings.TrimRight(rawSQL, " \t\r\n;")
	if !strings.HasSuffix(s, "*/") {
		return nil
	}
	open := strings.LastIndex(s, "/*")
	if open < 0 {
		return nil
	}
	body := strings.TrimSpace(s[open+2 : len(s)-2])
	return parseSqlcommenterBody(body)
}

func parseSqlcommenterBody(body string) []Tag {
	var out []Tag
	i := 0
	for i < len(body) {
		for i < len(body) && (body[i] == ' ' || body[i] == '\t' || body[i] == ',') {
			i++
		}
		if i >= len(body) {
			break
		}
		// key up to '='
		keyStart := i
		for i < len(body) && body[i] != '=' {
			i++
		}
		if i >= len(body) {
			return out
		}
		rawKey := strings.TrimSpace(body[keyStart:i])
		i++ // consume '='
		if i >= len(body) || body[i] != '\'' {
			return out
		}
		i++ // consume opening quote
		var val strings.Builder
		terminated := false
		for i < len(body) {
			c := body[i]
			if c == '\\' && i+1 < len(body) && body[i+1] == '\'' {
				val.WriteByte('\'')
				i += 2
				continue
			}
			if c == '\'' {
				terminated = true
				i++
				break
			}
			// Malformed raw `*/` in value: stop and discard rest.
			if c == '*' && i+1 < len(body) && body[i+1] == '/' {
				return out
			}
			val.WriteByte(c)
			i++
		}
		if !terminated {
			return out
		}
		k, err := url.QueryUnescape(rawKey)
		if err != nil {
			k = rawKey
		}
		v, err := url.QueryUnescape(val.String())
		if err != nil {
			v = val.String()
		}
		if k != "" {
			out = append(out, Tag{Key: k, Value: v, Format: FormatSqlcommenter})
		}
	}
	return out
}
