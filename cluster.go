package qshape

import "sort"

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
		Fingerprint     string  `json:"fingerprint"`
		Canonical       string  `json:"canonical"`
		Members         []Query `json:"members"`
		TotalCalls      int64   `json:"total_calls"`
		TotalExecTimeMs float64 `json:"total_exec_time_ms,omitempty"`
		MeanExecTimeMs  float64 `json:"mean_exec_time_ms,omitempty"`
		Rows            int64   `json:"rows,omitempty"`
	}
)

// Group clusters queries by canonical fingerprint. Queries that fail to
// parse become singleton clusters with empty Fingerprint. Output is
// sorted by descending TotalExecTimeMs (when any timing is present),
// otherwise by descending TotalCalls, with Fingerprint as the tiebreaker.
func Group(queries []Query) ([]Cluster, error) {
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
			canonical, derr := Normalize(q.Raw)
			if derr != nil {
				canonical = q.Raw
			}
			c = &Cluster{
				Fingerprint: fp,
				Canonical:   canonical,
			}
			groups[fp] = c
		}
		c.Members = append(c.Members, q)
		c.TotalCalls += q.Calls
		c.TotalExecTimeMs += q.TotalExecTimeMs
		c.Rows += q.Rows
	}

	out := make([]Cluster, 0, len(groups)+len(unparseable))
	for _, c := range groups {
		if c.TotalCalls > 0 {
			c.MeanExecTimeMs = c.TotalExecTimeMs / float64(c.TotalCalls)
		}
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
