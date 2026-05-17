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
	f, ok := DetectFormat(rawSQL)
	if !ok {
		return nil
	}
	switch f {
	case FormatHeader:
		return parseHeader(rawSQL)
	case FormatSqlcommenter:
		return parseSqlcommenter(rawSQL)
	case FormatMarginalia:
		return parseMarginalia(rawSQL)
	case FormatBareComment:
		return parseBareComment(rawSQL)
	}
	return nil
}

