package qshape

import (
	"sort"

	"github.com/boringsql/qshape/tags"
)

type (
	Query struct {
		Raw             string  `json:"raw"`
		QueryID         int64   `json:"queryid,omitempty"`
		Calls           int64   `json:"calls,omitempty"`
		TotalExecTimeMs float64 `json:"total_exec_time_ms,omitempty"`
		// Ignored by Group; a cluster's mean is derived from total/calls.
		MeanExecTimeMs   float64 `json:"mean_exec_time_ms,omitempty"`
		StddevExecTimeMs float64 `json:"stddev_exec_time_ms,omitempty"`
		Rows             int64   `json:"rows,omitempty"`
		// Temp blocks read and written by this statement: the sorts and hashes that
		// spilled out of work_mem. Pointers because a caller that did not collect them
		// (an older pg_stat_statements read, a fixture) means unknown, and 0 would say
		// the statement spilled nothing.
		TempBlksRead         *int64   `json:"temp_blks_read,omitempty"`
		TempBlksWritten      *int64   `json:"temp_blks_written,omitempty"`
		SharedBlksHit        *int64   `json:"shared_blks_hit,omitempty"`
		SharedBlksRead       *int64   `json:"shared_blks_read,omitempty"`
		SharedBlksDirtied    *int64   `json:"shared_blks_dirtied,omitempty"`
		SharedBlksWritten    *int64   `json:"shared_blks_written,omitempty"`
		SharedBlkReadTimeMs  *float64 `json:"shared_blk_read_time_ms,omitempty"`
		SharedBlkWriteTimeMs *float64 `json:"shared_blk_write_time_ms,omitempty"`
	}

	Cluster struct {
		Fingerprint     string  `json:"fingerprint"`
		Canonical       string  `json:"canonical"`
		Members         []Query `json:"members"`
		TotalCalls      int64   `json:"total_calls"`
		TotalExecTimeMs float64 `json:"total_exec_time_ms,omitempty"`
		MeanExecTimeMs  float64 `json:"mean_exec_time_ms,omitempty"`
		Rows            int64   `json:"rows,omitempty"`
		// Summed over the members, and nil unless EVERY member carried them: a partial
		// sum understates the spill, which is the direction that makes "raise work_mem"
		// advice wrong.
		TempBlksRead    *int64                  `json:"temp_blks_read,omitempty"`
		TempBlksWritten *int64                  `json:"temp_blks_written,omitempty"`
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
//
// Fingerprinting already uses all cores; callers should not fan Group out across their
// own worker pool.
func Group(queries []Query) ([]Cluster, error) {
	return GroupWithPolicy(queries, tags.DefaultPolicy())
}

// sumTempBlocks totals an optional counter over a cluster's members, and returns nil as
// soon as one member does not carry it. A partial sum would read as a smaller spill than
// really happened, and understating a spill is what makes "raise work_mem" advice point
// the wrong way.
func sumTempBlocks(members []Query, pick func(Query) *int64) *int64 {
	if len(members) == 0 {
		return nil
	}
	var total int64
	for _, m := range members {
		v := pick(m)
		if v == nil {
			return nil
		}
		total += *v
	}
	return &total
}

func GroupWithPolicy(queries []Query, policy *tags.Policy) ([]Cluster, error) {
	groups := make(map[string]*Cluster)
	var unparseable []Cluster

	// Fingerprinting is the expensive half, so it runs across cores first; grouping
	// stays sequential over the original order, which consumers hash.
	shapes := fingerprintAll(queries)
	// The text each fingerprint was computed from, reused by setCanonical instead of
	// normalizing the same SQL twice. Keyed by raw SQL: identical raw text normalizes
	// identically.
	canonicalByRaw := make(map[string]string, len(queries))

	for i, q := range queries {
		fp, canonical, err := shapes[i].fingerprint, shapes[i].canonical, shapes[i].err
		if err != nil {
			unparseable = append(unparseable, Cluster{
				Fingerprint:     "",
				Canonical:       q.Raw,
				Members:         []Query{q},
				TotalCalls:      q.Calls,
				TotalExecTimeMs: q.TotalExecTimeMs,
				Rows:            q.Rows,
			})
			continue
		}
		canonicalByRaw[q.Raw] = canonical
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

	for _, c := range groups {
		c.TempBlksRead = sumTempBlocks(c.Members, func(q Query) *int64 { return q.TempBlksRead })
		c.TempBlksWritten = sumTempBlocks(c.Members, func(q Query) *int64 { return q.TempBlksWritten })
	}
	for i := range unparseable {
		unparseable[i].TempBlksRead = unparseable[i].Members[0].TempBlksRead
		unparseable[i].TempBlksWritten = unparseable[i].Members[0].TempBlksWritten
		if unparseable[i].TotalCalls > 0 {
			unparseable[i].MeanExecTimeMs = unparseable[i].TotalExecTimeMs / float64(unparseable[i].TotalCalls)
		}
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
		setCanonical(c, canonicalByRaw)
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

	// Fingerprint is "" for every unparseable cluster, so ties must be broken down to
	// the first member's QueryID; past that the clusters are indistinguishable.
	sort.Slice(out, func(i, j int) bool {
		if hasTiming && out[i].TotalExecTimeMs != out[j].TotalExecTimeMs {
			return out[i].TotalExecTimeMs > out[j].TotalExecTimeMs
		}
		if out[i].TotalCalls != out[j].TotalCalls {
			return out[i].TotalCalls > out[j].TotalCalls
		}
		if out[i].Fingerprint != out[j].Fingerprint {
			return out[i].Fingerprint < out[j].Fingerprint
		}
		if out[i].Canonical != out[j].Canonical {
			return out[i].Canonical < out[j].Canonical
		}
		return firstQueryID(out[i]) < firstQueryID(out[j])
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
func setCanonical(c *Cluster, canonicalByRaw map[string]string) {
	if len(c.Members) == 0 {
		return
	}
	raw := c.Members[0].Raw
	// A miss means a cluster built outside GroupWithPolicy; raw is Normalize's own
	// fallback.
	canonical, ok := canonicalByRaw[raw]
	if !ok {
		canonical = raw
	}
	c.Canonical = canonical
}

// firstQueryID is the last sort tiebreaker; 0 keeps an empty cluster safe.
func firstQueryID(c Cluster) int64 {
	if len(c.Members) == 0 {
		return 0
	}
	return c.Members[0].QueryID
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
