package qshape

import (
	"sort"

	"github.com/boringsql/qshape/tags"
)

type (
	Query struct {
		Raw              string  `json:"raw"`
		QueryID          int64   `json:"queryid,omitempty"`
		Calls            int64   `json:"calls,omitempty"`
		TotalExecTimeMs  float64 `json:"total_exec_time_ms,omitempty"`
		MeanExecTimeMs   float64 `json:"mean_exec_time_ms,omitempty"`
		StddevExecTimeMs float64 `json:"stddev_exec_time_ms,omitempty"`
		Rows             int64   `json:"rows,omitempty"`
	}

	Cluster struct {
		Fingerprint     string                  `json:"fingerprint"`
		Canonical       string                  `json:"canonical"`
		Members         []Query                 `json:"members"`
		TotalCalls      int64                   `json:"total_calls"`
		TotalExecTimeMs float64                 `json:"total_exec_time_ms,omitempty"`
		MeanExecTimeMs  float64                 `json:"mean_exec_time_ms,omitempty"`
		Rows            int64                   `json:"rows,omitempty"`
		Params          []ParamAttribution      `json:"params,omitempty"`
		Owners          map[string]string       `json:"owners,omitempty"`
		RegresqlMeta    map[string]string       `json:"regresql_meta,omitempty"`
		DynamicTagKeys  []DynamicKeyObservation `json:"dynamic_tag_keys,omitempty"`
	}

	DynamicKeyObservation struct {
		Key                  string `json:"key"`
		ValueCardinalitySeen int    `json:"value_cardinality_seen"`
	}

	ParamAttribution struct {
		Position   int    `json:"position"`
		Schema     string `json:"schema,omitempty"`
		Table      string `json:"table,omitempty"`
		Column     string `json:"column,omitempty"`
		Confidence string `json:"confidence"`
		Note       string `json:"note,omitempty"`
	}
)

// Group clusters queries by canonical fingerprint. Queries that fail to
// parse become singleton clusters with empty Fingerprint. Output is
// sorted by descending TotalExecTimeMs (when any timing is present),
// otherwise by descending TotalCalls, with Fingerprint as the tiebreaker.
// Each cluster's Members are sorted by QueryID, then Raw, and Canonical is
// the normalized form of the first member after that sort.
func Group(queries []Query) ([]Cluster, error) {
	return GroupWithPolicy(queries, tags.DefaultPolicy())
}

func GroupWithPolicy(queries []Query, policy *tags.Policy) ([]Cluster, error) {
	groups := make(map[string]*Cluster)
	var unparseable []Cluster

	for _, q := range queries {
		fp, err := Fingerprint(q.Raw)
		if err != nil {
			unparseable = append(unparseable, Cluster{
				Fingerprint:     "",
				Canonical:       q.Raw,
				Members:         []Query{q},
				TotalCalls:      q.Calls,
				TotalExecTimeMs: q.TotalExecTimeMs,
				Rows:            q.Rows,
				MeanExecTimeMs:  q.MeanExecTimeMs,
			})
			continue
		}
		c, ok := groups[fp]
		if !ok {
			// Canonical is derived once the members are sorted, below
			c = &Cluster{Fingerprint: fp}
			groups[fp] = c
		}
		c.Members = append(c.Members, q)
		c.TotalCalls += q.Calls
		c.TotalExecTimeMs += q.TotalExecTimeMs
		c.Rows += q.Rows
	}

	if policy == nil {
		policy = tags.DefaultPolicy()
	}
	out := make([]Cluster, 0, len(groups)+len(unparseable))
	for _, c := range groups {
		if c.TotalCalls > 0 {
			c.MeanExecTimeMs = c.TotalExecTimeMs / float64(c.TotalCalls)
		}
		sortMembers(c)
		setCanonical(c)
		applyTags(c, policy)
		out = append(out, *c)
	}
	out = append(out, unparseable...)

	hasTiming := false
	for _, c := range out {
		if c.TotalExecTimeMs > 0 {
			hasTiming = true
			break
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if hasTiming && out[i].TotalExecTimeMs != out[j].TotalExecTimeMs {
			return out[i].TotalExecTimeMs > out[j].TotalExecTimeMs
		}
		if out[i].TotalCalls != out[j].TotalCalls {
			return out[i].TotalCalls > out[j].TotalCalls
		}
		return out[i].Fingerprint < out[j].Fingerprint
	})
	return out, nil
}

// sortMembers makes member order deterministic (QueryID, then Raw):
// pg_stat_statements ties permute between captures, and consumers hashing
// the member list would read that as a change. Also pins which member
// applyTags reads.
func sortMembers(c *Cluster) {
	sort.Slice(c.Members, func(i, j int) bool {
		if c.Members[i].QueryID != c.Members[j].QueryID {
			return c.Members[i].QueryID < c.Members[j].QueryID
		}
		return c.Members[i].Raw < c.Members[j].Raw
	})
}

// setCanonical pins Canonical to the sorted-first member: members sharing a
// fingerprint can still normalize differently (IN lists of different lengths
// cluster together), and attribution and stub generation run off this field.
func setCanonical(c *Cluster) {
	if len(c.Members) == 0 {
		return
	}
	canonical, err := Normalize(c.Members[0].Raw)
	if err != nil {
		canonical = c.Members[0].Raw
	}
	c.Canonical = canonical
}

func applyTags(c *Cluster, policy *tags.Policy) {
	if len(c.Members) == 0 {
		return
	}
	extracted := tags.Extract(c.Members[0].Raw)
	if len(extracted) == 0 {
		return
	}
	classified := tags.Classify(extracted, policy)
	c.Owners = classified.Owners
	c.RegresqlMeta = classified.RegresqlMeta
	if len(classified.DynamicKeys) > 0 {
		c.DynamicTagKeys = make([]DynamicKeyObservation, len(classified.DynamicKeys))
		for i, d := range classified.DynamicKeys {
			c.DynamicTagKeys[i] = DynamicKeyObservation{Key: d.Key, ValueCardinalitySeen: d.ValueCardinalitySeen}
		}
		sort.Slice(c.DynamicTagKeys, func(i, j int) bool {
			return c.DynamicTagKeys[i].Key < c.DynamicTagKeys[j].Key
		})
	}
}
