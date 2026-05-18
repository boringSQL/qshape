// Package tags extracts and classifies structured comments embedded in SQL.
package tags

type (
	Format int

	Tag struct {
		Key    string
		Value  string
		Format Format
	}

	DynamicKeyObservation struct {
		Key                  string
		ValueCardinalitySeen int
	}

	ClassifiedTags struct {
		Owners       map[string]string
		RegresqlMeta map[string]string
		DynamicKeys  []DynamicKeyObservation
	}

	Policy struct {
		Stable                      map[string]struct{}
		Deny                        map[string]struct{}
		VendorMap                   map[string]string
		Reserved                    map[string]struct{}
		CardinalityPromoteThreshold int
	}
)

const (
	FormatHeader Format = iota + 1
	FormatSqlcommenter
	FormatMarginalia
	FormatBareComment
)

func Extract(rawSQL string) []Tag {
	if t := parseHeader(rawSQL); len(t) > 0 {
		return t
	}
	if t := parseSqlcommenter(rawSQL); len(t) > 0 {
		return t
	}
	if t := parseMarginalia(rawSQL); len(t) > 0 {
		return t
	}
	if t := parseBareComment(rawSQL); len(t) > 0 {
		return t
	}
	return nil
}

