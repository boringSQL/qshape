package main

import "strings"

// Operand kinds produced by classifyOperand.
const (
	operandColumn     = "column"
	operandParam      = "param"
	operandArrayParam = "arrayparam"
	operandArrayLit   = "arraylit"
	operandExpr       = "expr"
	operandOther      = "other"
)

type operand struct {
	kind   string
	alias  string
	column string
	poss   []int
	text   string
}

// cmpOps are the comparison operators that act as split points. Distance
// operators (<-> <#> <=>) are deliberately absent: an ORDER BY distance is not
// a column/parameter equality.
var cmpOps = map[string]bool{
	"!~~*": true, "!~~": true, "~~*": true, "~~": true, "!~*": true, "!~": true, "~*": true, "~": true,
	"<>": true, "!=": true, "<=": true, ">=": true, "@>": true, "<@": true, "&&": true, "=": true, "<": true, ">": true,
}

const isDistinctFrom = "IS DISTINCT FROM"

// forEachComparison calls f for every `left op right` comparison in a plan
// condition. AND/OR are split first, then a single comparison operator is
// located at the current depth. NOT and redundant parentheses are unwrapped.
func forEachComparison(expr string, f func(left, right string)) {
	s := strings.TrimSpace(expr)
	for {
		t := strings.TrimSpace(s)
		if rest, ok := strings.CutPrefix(t, "NOT "); ok {
			rest = strings.TrimSpace(rest)
			if strings.HasPrefix(rest, "(") {
				s = rest
				continue
			}
		}
		if isWrapped(t) {
			s = t[1 : len(t)-1]
			continue
		}
		break
	}
	s = strings.TrimSpace(s)
	if left, right, ok := splitTopLevelBool(s); ok {
		forEachComparison(left, f)
		forEachComparison(right, f)
		return
	}
	if i, op, ok := findTopLevelOp(s); ok {
		f(s[:i], s[i+len(op):])
	}
}

// topLevel calls visit for each byte of s outside quotes and parentheses.
// visit returns the index to resume from, or -1 to stop.
func topLevel(s string, visit func(i int) int) {
	depth := 0
	for i := 0; i < len(s); {
		switch c := s[i]; {
		case c == '\'' || c == '"':
			i = skipQuoted(s, i)
		case c == '(':
			depth++
			i++
		case c == ')':
			depth--
			i++
		case depth != 0:
			i++
		default:
			if i = visit(i); i < 0 {
				return
			}
		}
	}
}

// splitTopLevelBool splits at the first top-level AND/OR.
func splitTopLevelBool(s string) (left, right string, ok bool) {
	topLevel(s, func(i int) int {
		if !isIdentStart(s[i]) {
			return i + 1
		}
		j := identEnd(s, i)
		if w := strings.ToUpper(s[i:j]); w == "AND" || w == "OR" {
			left, right, ok = s[:i], s[j:], true
			return -1
		}
		return j
	})
	return left, right, ok
}

// findTopLevelOp locates the first comparison operator at depth 0. Operator
// characters are consumed as one maximal run, so `->>` is never read as `>`.
// A comparison needs a non-empty left operand; an operator run at the start is
// unary (e.g. bitwise NOT `~flags`), not a split.
func findTopLevelOp(s string) (at int, op string, ok bool) {
	topLevel(s, func(i int) int {
		hasLeft := strings.TrimSpace(s[:i]) != ""
		if hasLeft && wordBoundary(s, i) && hasWord(s[i:], isDistinctFrom) {
			at, op, ok = i, s[i:i+len(isDistinctFrom)], true
			return -1
		}
		if !isOpChar(s[i]) {
			return i + 1
		}
		j := i
		for j < len(s) && isOpChar(s[j]) {
			j++
		}
		if hasLeft && cmpOps[s[i:j]] {
			at, op, ok = i, s[i:j], true
			return -1
		}
		return j
	})
	return at, op, ok
}

// hasWord reports whether s starts with word (case-insensitive) followed by a
// non-identifier character.
func hasWord(s, word string) bool {
	return len(s) >= len(word) && strings.EqualFold(s[:len(word)], word) &&
		(len(s) == len(word) || !isIdentChar(s[len(word)]))
}

func classifyOperand(raw string) operand {
	s := stripOperand(raw)
	if inner, ok := anyInner(s); ok {
		t := stripOperand(inner)
		if pos, ok := parseBareParam(t); ok {
			return operand{kind: operandArrayParam, poss: []int{pos}, text: raw}
		}
		if poss, ok := parseArrayLiteral(t); ok {
			return operand{kind: operandArrayLit, poss: poss, text: raw}
		}
		return operand{kind: operandOther, text: raw}
	}
	if pos, ok := parseBareParam(s); ok {
		return operand{kind: operandParam, poss: []int{pos}, text: raw}
	}
	if alias, col, ok := parseColumnRef(s); ok {
		return operand{kind: operandColumn, alias: alias, column: col, text: raw}
	}
	if poss := scanParams(s); len(poss) > 0 {
		return operand{kind: operandExpr, poss: poss, text: s}
	}
	return operand{kind: operandOther, text: raw}
}

// stripOperand removes redundant outer parentheses and trailing `::type`
// casts, repeatedly, so `((status)::text)` becomes `status`.
func stripOperand(s string) string {
	for {
		t := strings.TrimSpace(s)
		if isWrapped(t) {
			s = t[1 : len(t)-1]
			continue
		}
		if cut, ok := cutTrailingCast(t); ok {
			s = cut
			continue
		}
		return t
	}
}

// anyInner recognises `ANY (...)` / `ALL (...)` and returns the parenthesised
// body.
func anyInner(s string) (string, bool) {
	for _, kw := range []string{"ANY", "ALL"} {
		if !hasWord(s, kw) {
			continue
		}
		rest := strings.TrimSpace(s[len(kw):])
		if isWrapped(rest) {
			return rest[1 : len(rest)-1], true
		}
	}
	return "", false
}

func parseBareParam(s string) (int, bool) {
	if len(s) < 2 || s[0] != '$' {
		return 0, false
	}
	n := 0
	for i := 1; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	return n, true
}

// parseArrayLiteral recognises `ARRAY[$1, $2, …]` and returns the positions
// of the elements that are bare params, in order. An element such as
// `($1 + 1)` is not compared to the column directly, so it is left out.
func parseArrayLiteral(s string) ([]int, bool) {
	if len(s) < 7 || !strings.EqualFold(s[:6], "ARRAY[") || !strings.HasSuffix(s, "]") {
		return nil, false
	}
	body := s[6 : len(s)-1]
	var out []int
	start := 0
	addElem := func(end int) {
		if pos, ok := parseBareParam(stripOperand(body[start:end])); ok {
			out = append(out, pos)
		}
	}
	topLevel(body, func(i int) int {
		if body[i] == ',' {
			addElem(i)
			start = i + 1
		}
		return i + 1
	})
	addElem(len(body))
	return out, len(out) > 0
}

// parseColumnRef recognises `col` or `alias.col` after stripping is done.
func parseColumnRef(s string) (string, string, bool) {
	alias, col, qualified := strings.Cut(s, ".")
	if !qualified {
		return "", s, isIdent(s) && !exprKeywords[strings.ToLower(s)]
	}
	return alias, col, isIdent(alias) && isIdent(col)
}

// cutTrailingCast removes a top-level `::type` that runs to the end of s and
// applies to the whole of what precedes it: a paren group, a column or a
// param. A cast on the last term of an expression (`now() - ($1)::interval`)
// or an inner cast (`($1)::date + 1`) is left alone.
func cutTrailingCast(s string) (string, bool) {
	cut, ok := s, false
	topLevel(s, func(i int) int {
		if !strings.HasPrefix(s[i:], "::") {
			return i + 1
		}
		if skipCast(s, i+2) == len(s) && castTarget(strings.TrimSpace(s[:i])) {
			cut, ok = strings.TrimSpace(s[:i]), true
			return -1
		}
		return i + 2
	})
	return cut, ok
}

func castTarget(s string) bool {
	if isWrapped(s) {
		return true
	}
	if _, ok := parseBareParam(s); ok {
		return true
	}
	_, _, ok := parseColumnRef(s)
	return ok
}

var castContinuation = map[string]bool{
	"with": true, "without": true, "time": true, "zone": true,
	"varying": true, "precision": true, "double": true, "character": true,
	"bit": true, "national": true,
}

// skipCast consumes a type name starting just after a `::`. The type grammar
// is consumed narrowly (identifier, dots, modifiers, brackets, continuation
// words) so it cannot run past the end of the type.
func skipCast(s string, i int) int {
	seenIdent := false
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '.':
			i++
		case strings.HasPrefix(s[i:], "[]"):
			i += 2
		case c == '(':
			depth := 0
			for i < len(s) {
				switch s[i] {
				case '(':
					depth++
				case ')':
					depth--
				}
				i++
				if depth == 0 {
					break
				}
			}
		case c == '"':
			i = skipQuoted(s, i)
			seenIdent = true
		case isIdentStart(c):
			j := identEnd(s, i)
			if seenIdent && !castContinuation[strings.ToLower(s[i:j])] {
				return i
			}
			seenIdent = true
			i = j
		default:
			return i
		}
	}
	return i
}

// exprHasOtherColumn reports whether an expression refers to any column, or to
// a subplan output. Function names, allowlisted keywords and cast types do not
// count; `$N` is skipped because `$` is not an identifier start.
func exprHasOtherColumn(s string) bool {
	if strings.Contains(s, "(InitPlan ") || strings.Contains(s, "(SubPlan ") {
		return true
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '\'' || c == '"':
			i = skipQuoted(s, i)
		case strings.HasPrefix(s[i:], "::"):
			i = skipCast(s, i+2)
		case isIdentStart(c):
			j := identEnd(s, i)
			if !strings.HasPrefix(strings.TrimLeft(s[j:], " "), "(") && !exprKeywords[strings.ToLower(s[i:j])] {
				return true
			}
			i = j
		default:
			i++
		}
	}
	return false
}

// skipQuoted returns the index just past the '...' or "..." literal starting
// at i, honoring doubled-quote escapes.
func skipQuoted(s string, i int) int {
	quote := s[i]
	for i++; i < len(s); i++ {
		if s[i] != quote {
			continue
		}
		if i+1 < len(s) && s[i+1] == quote {
			i++
			continue
		}
		return i + 1
	}
	return i
}

// scanParams returns each $N in s, in order, skipping '...' and "..."
// literals so a `$` inside a string is never read as a parameter.
func scanParams(s string) []int {
	var out []int
	for i := 0; i < len(s); {
		switch s[i] {
		case '\'', '"':
			i = skipQuoted(s, i)
			continue
		case '$':
			j, n := i+1, 0
			for ; j < len(s) && s[j] >= '0' && s[j] <= '9'; j++ {
				n = n*10 + int(s[j]-'0')
			}
			if j > i+1 {
				out = append(out, n)
				i = j
				continue
			}
		}
		i++
	}
	return out
}

// isWrapped reports whether the first and last characters are a matched pair
// enclosing the whole string.
func isWrapped(s string) bool {
	return len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' && balanced(s[1:len(s)-1])
}

func balanced(s string) bool {
	depth := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '\'', '"':
			i = skipQuoted(s, i)
			continue
		case '(':
			depth++
		case ')':
			if depth--; depth < 0 {
				return false
			}
		}
		i++
	}
	return depth == 0
}

func wordBoundary(s string, i int) bool {
	return i == 0 || !isIdentChar(s[i-1])
}

func identEnd(s string, i int) int {
	for i < len(s) && isIdentChar(s[i]) {
		i++
	}
	return i
}

func isIdent(s string) bool {
	return s != "" && isIdentStart(s[0]) && identEnd(s, 0) == len(s)
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}

func isOpChar(c byte) bool {
	return strings.IndexByte("=<>!~@&|+-*/%^#", c) >= 0
}

// exprKeywords are bare words that are not column references in an
// expression. Function names are handled separately (identifier followed by
// `(`).
var exprKeywords = map[string]bool{
	"current_date": true, "current_time": true, "current_timestamp": true,
	"localtime": true, "localtimestamp": true, "current_user": true,
	"session_user": true, "current_schema": true, "current_catalog": true,
	"current_role": true, "system_user": true, "user": true,
	"true": true, "false": true, "null": true,
	"case": true, "when": true, "then": true, "else": true, "end": true,
	"at": true, "time": true, "zone": true, "interval": true,
	"and": true, "or": true, "not": true, "is": true, "distinct": true,
	"from": true, "any": true, "all": true, "array": true,
}
