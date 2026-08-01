package qshape

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Prettify re-indents a statement for display: line breaks at clause boundaries,
// indentation by nesting depth. It changes WHITESPACE ONLY. Every token is copied
// verbatim from the input, so the result parses to the same tree and fingerprints to
// the same value as what went in; TestPrettifyPreservesFingerprint pins that over the
// captured corpus.
//
// It is a display transform, never a normalization. The canonical single-line form
// stays the fingerprint input and the storage format; this is what a human reads.
//
// Built on the PostgreSQL scanner rather than on pattern matching over the text,
// because only the real lexer knows that the FROM in `WHERE note = 'sent FROM x'` is
// four letters inside a string constant and not a clause. The same goes for dollar
// quoting, comments and quoted identifiers containing keywords; a regex formatter
// corrupts all four, silently.
func Prettify(sql string) (string, error) {
	res, err := pg_query.Scan(sql)
	if err != nil {
		return "", err
	}
	toks := res.Tokens
	if len(toks) == 0 {
		return strings.TrimSpace(sql), nil
	}

	texts := make([]string, len(toks))
	upper := make([]string, len(toks))
	for i, t := range toks {
		texts[i] = sql[t.Start:t.End]
		upper[i] = strings.ToUpper(texts[i])
	}
	breaks := breakPoints(toks, upper)

	var (
		b     strings.Builder
		depth int
		// multiline records, per open paren, whether that group has already wrapped, so
		// its closing paren returns to its own line only when the group actually spans
		// several; `count(*)` must not become three lines.
		multiline  []bool
		lineIsOpen bool // something has been written on the current line
	)
	indent := func() {
		b.WriteString(strings.Repeat("  ", depth))
	}
	newline := func() {
		b.WriteByte('\n')
		indent()
		lineIsOpen = false
		for i := range multiline {
			multiline[i] = true
		}
	}

	for i, tok := range toks {
		text := texts[i]

		switch text {
		case ")":
			if depth > 0 {
				wrapped := multiline[len(multiline)-1]
				multiline = multiline[:len(multiline)-1]
				depth--
				if wrapped {
					newline()
				}
			}
		default:
			if breaks[i] && lineIsOpen {
				newline()
			}
		}

		if lineIsOpen && needsSpace(toks, texts, i) {
			b.WriteByte(' ')
		} else if !lineIsOpen && b.Len() == 0 {
			indent()
		}
		b.WriteString(text)
		lineIsOpen = true

		if text == "(" {
			depth++
			multiline = append(multiline, false)
		}
		// A line comment swallows everything to the end of the line, so the next token
		// can never share it. This is the one case where layout is correctness.
		if tok.Token == pg_query.Token_SQL_COMMENT {
			newline()
		}
	}

	out := b.String()
	// Verify, do not trust. Every rule below about where a space may be dropped is a
	// claim about how two tokens lex when they touch, and getting one wrong silently
	// changes the statement rather than breaking it loudly: `SELEC. 0` re-joined as
	// `SELEC.0` turns three tokens into two, because `.0` lexes as a float. So the
	// output is re-scanned and compared, and anything that does not match token for
	// token is discarded in favour of the input. A user seeing an unformatted statement
	// is a cosmetic loss; a user seeing a statement that is not the one that ran is not.
	if !sameTokens(sql, toks, out) {
		return sql, nil
	}
	return out, nil
}

// sameTokens reports whether laying the statement out preserved its token stream exactly.
func sameTokens(sql string, before []*pg_query.ScanToken, pretty string) bool {
	res, err := pg_query.Scan(pretty)
	if err != nil {
		return false
	}
	if len(res.Tokens) != len(before) {
		return false
	}
	for i, tok := range res.Tokens {
		if pretty[tok.Start:tok.End] != sql[before[i].Start:before[i].End] {
			return false
		}
	}
	return true
}

// clauseKeywords start a new line. Deliberately conservative: a keyword that only
// sometimes begins a clause (ON, AS, IN) stays inline, because a wrong break reads as a
// second clause that is not there.
var clauseKeywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "HAVING": true,
	"GROUP": true, "ORDER": true, "LIMIT": true, "OFFSET": true, "FETCH": true,
	"WINDOW": true, "UNION": true, "INTERSECT": true, "EXCEPT": true,
	"INSERT": true, "UPDATE": true, "DELETE": true, "VALUES": true,
	"SET": true, "RETURNING": true, "WITH": true,
	"AND": true, "OR": true,
}

// joinPrefixes precede JOIN. The break belongs before the whole phrase, so `LEFT OUTER
// JOIN x` starts a line at LEFT rather than leaving `LEFT OUTER` stranded on the one
// above.
var joinPrefixes = map[string]bool{
	"LEFT": true, "RIGHT": true, "FULL": true, "INNER": true,
	"CROSS": true, "OUTER": true, "NATURAL": true,
}

// breakPoints marks the tokens that begin a new line.
func breakPoints(toks []*pg_query.ScanToken, upper []string) []bool {
	out := make([]bool, len(toks))
	// BETWEEN x AND y: that AND joins one predicate's bounds, so breaking on it splits
	// an expression rather than a clause list. Counted per open BETWEEN, since two can
	// nest across paren depths.
	between := 0
	for i, up := range upper {
		switch {
		case up == "BETWEEN":
			between++
		case up == "AND" && between > 0:
			between--
		case up == "JOIN":
			start := i
			for start > 0 && joinPrefixes[upper[start-1]] {
				start--
			}
			out[start] = true
		case clauseKeywords[up]:
			// Only when it is really the keyword: an identifier or a quoted name can
			// carry the same letters, and a column called "set" must not split a line.
			if toks[i].KeywordKind != pg_query.KeywordKind_NO_KEYWORD {
				out[i] = true
			}
		}
	}
	return out
}

// needsSpace decides the separator between two adjacent tokens. Aesthetic only: the
// tokens themselves are never altered, so the worst a wrong answer costs is an ugly
// line, never a changed statement.
func needsSpace(toks []*pg_query.ScanToken, texts []string, i int) bool {
	if i == 0 {
		return false
	}
	prev, cur := texts[i-1], texts[i]

	// A qualifying dot closes up only between name-like tokens (`t.col`, `public.orders`,
	// `t.*`). Next to a number it must keep its space: `1` `.` and `.` `0` would lex back
	// as the float constants `1.` and `.0`, fusing three tokens into two.
	if cur == "." {
		return !nameLike(toks[i-1], prev)
	}
	if prev == "." {
		return !nameLike(toks[i], cur)
	}

	// Brackets close up on both sides: an array type is `bigint[]`, a subscript is
	// `a[$1]`, a constructor is `ARRAY[$1, $2]`. Spaced out they read as separate
	// operators, which is what `bigint [ ]` looks like.
	switch cur {
	case ",", ")", ";", "::", "[", "]":
		return false
	}
	switch prev {
	case "(", "::", "[":
		return false
	}
	if cur == "(" {
		// A function call closes up (`count(*)`, `coalesce(a, b)`). Everything else keeps
		// its space: a keyword taking a parenthesized list (`IN (...)`, `VALUES (...)`)
		// and, just as importantly, an operator (`= (...)`, `->> (...)`) — an operator is
		// NO_KEYWORD like an identifier is, so testing the keyword kind alone renders
		// `=(` and `->>(`.
		if toks[i-1].Token == pg_query.Token_IDENT ||
			toks[i-1].KeywordKind == pg_query.KeywordKind_COL_NAME_KEYWORD {
			return false
		}
	}
	return true
}

// nameLike reports whether a token can sit against a qualifying dot without lexing into
// it: an identifier, a keyword used as a name, or the `*` of `t.*`. Numeric constants
// are the case this excludes, and quoted identifiers carry their own delimiters so they
// are safe either way.
func nameLike(tok *pg_query.ScanToken, text string) bool {
	if text == "*" {
		return true
	}
	switch tok.Token {
	case pg_query.Token_IDENT:
		return true
	}
	return tok.KeywordKind != pg_query.KeywordKind_NO_KEYWORD
}
