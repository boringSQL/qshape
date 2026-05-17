package tags

import (
	"regexp"
	"strings"
)

var (
	reHeaderKV       = regexp.MustCompile(`^[ \t]*--[ \t]*([A-Za-z_][A-Za-z0-9_.-]*)[ \t]*:[ \t]*(.*?)[ \t]*$`)
	reSqlcCardinality = regexp.MustCompile(`[ \t]+:(one|many|exec|execrows|execresult|batchexec|batchmany|batchone|copyfrom)$`)
)

func parseHeader(rawSQL string) []Tag {
	var out []Tag
	for line := range strings.SplitSeq(rawSQL, "\n") {
		line = strings.TrimRight(line, "\r")
		t := strings.TrimLeft(line, " \t")
		if !strings.HasPrefix(t, "--") {
			if strings.TrimSpace(t) == "" {
				continue
			}
			break
		}
		m := reHeaderKV.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		key, val := m[1], m[2]
		if key == "name" {
			if c := reSqlcCardinality.FindStringSubmatch(val); c != nil {
				val = strings.TrimRight(val[:len(val)-len(c[0])], " \t")
				out = append(out, Tag{Key: "cardinality", Value: c[1], Format: FormatHeader})
			}
		}
		out = append(out, Tag{Key: key, Value: val, Format: FormatHeader})
	}
	return out
}
