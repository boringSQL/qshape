package tags

import (
	"regexp"
	"strings"
)

var (
	reHeaderLine        = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*[A-Za-z_][A-Za-z0-9_.-]*[ \t]*:[ \t]*\S`)
	reSqlcommenterBlock = regexp.MustCompile(`/\*\s*[A-Za-z_][A-Za-z0-9_.%-]*='[^']*'(?:\s*,\s*[A-Za-z_][A-Za-z0-9_.%-]*='[^']*')*\s*\*/\s*;?\s*$`)
	reMarginaliaPair    = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_.-]*\s*[:=]\s*[^,*]+`)
	reBlockComment      = regexp.MustCompile(`/\*([\s\S]*?)\*/`)
)

// DetectFormat returns the tag format used by rawSQL, or (0, false) if none.
// Order: header → sqlcommenter → marginalia → bare-comment.
func DetectFormat(rawSQL string) (Format, bool) {
	if reHeaderLine.MatchString(leadingComments(rawSQL)) {
		return FormatHeader, true
	}
	if reSqlcommenterBlock.MatchString(strings.TrimRight(rawSQL, " \t\r\n;")) {
		return FormatSqlcommenter, true
	}
	if f, ok := detectMarginalia(rawSQL); ok {
		return f, true
	}
	if detectBareComment(rawSQL) {
		return FormatBareComment, true
	}
	return 0, false
}

func leadingComments(s string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(s, "\n") {
		t := strings.TrimLeft(line, " \t")
		if strings.HasPrefix(t, "--") {
			b.WriteString(line)
			b.WriteByte('\n')
			continue
		}
		if strings.TrimSpace(t) == "" {
			continue
		}
		break
	}
	return b.String()
}

func detectMarginalia(s string) (Format, bool) {
	for _, block := range edgeBlockComments(s) {
		body := strings.TrimSpace(block)
		if body == "" {
			continue
		}
		if reMarginaliaPair.MatchString(body) && (strings.Contains(body, ":") || strings.Contains(body, "=")) {
			return FormatMarginalia, true
		}
	}
	return 0, false
}

func detectBareComment(s string) bool {
	for _, block := range edgeBlockComments(s) {
		body := strings.TrimSpace(block)
		if body == "" {
			continue
		}
		if !strings.ContainsAny(body, ":=,") {
			return true
		}
	}
	t := strings.TrimLeft(s, " \t\r\n")
	if strings.HasPrefix(t, "--") {
		line := strings.TrimSpace(strings.TrimPrefix(strings.SplitN(t, "\n", 2)[0], "--"))
		if line != "" && !strings.ContainsAny(line, ":=,") {
			return true
		}
	}
	return false
}

func edgeBlockComments(s string) []string {
	var out []string
	trimmed := strings.TrimLeft(s, " \t\r\n")
	if m := reBlockComment.FindStringSubmatch(trimmed); m != nil && strings.HasPrefix(trimmed, "/*") {
		out = append(out, m[1])
	}
	trimmedR := strings.TrimRight(s, " \t\r\n;")
	if strings.HasSuffix(trimmedR, "*/") {
		if idx := strings.LastIndex(trimmedR, "/*"); idx >= 0 {
			out = append(out, trimmedR[idx+2:len(trimmedR)-2])
		}
	}
	return out
}
