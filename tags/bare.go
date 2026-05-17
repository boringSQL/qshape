package tags

import (
	"regexp"
	"strings"
)

var reBareSanitize = regexp.MustCompile(`[^a-z0-9-]+`)

func parseBareComment(rawSQL string) []Tag {
	body := bareCommentBody(rawSQL)
	if body == "" {
		return nil
	}
	name := sanitizeBareName(body)
	if name == "" {
		return nil
	}
	return []Tag{{Key: "name", Value: name, Format: FormatBareComment}}
}

func bareCommentBody(s string) string {
	for _, block := range edgeBlockComments(s) {
		body := strings.TrimSpace(block)
		if body != "" && !strings.ContainsAny(body, ":=,") {
			return body
		}
	}
	t := strings.TrimLeft(s, " \t\r\n")
	if strings.HasPrefix(t, "--") {
		line := strings.TrimSpace(strings.TrimPrefix(strings.SplitN(t, "\n", 2)[0], "--"))
		if line != "" && !strings.ContainsAny(line, ":=,") {
			return line
		}
	}
	return ""
}

func sanitizeBareName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = reBareSanitize.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	if len(s) > 80 {
		s = strings.TrimRight(s[:80], "-")
	}
	return s
}
