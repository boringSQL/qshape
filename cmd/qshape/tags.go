package main

import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/boringsql/qshape"
	"github.com/boringsql/qshape/tags"
	"github.com/spf13/cobra"
)

func tagsCmd() *cobra.Command {
	var (
		inPath          string
		byKey           string
		showPromotable  bool
		policyFilePath  string
	)
	cmd := &cobra.Command{
		Use:   "tags",
		Short: "Inspect / re-classify tags on a clusters.json",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runTags(inPath, byKey, showPromotable, policyFilePath, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input clusters.json (default: stdin)")
	cmd.Flags().StringVar(&byKey, "by", "", "group clusters by this Owners key (e.g. application, controller)")
	cmd.Flags().BoolVar(&showPromotable, "show-promotable", false, "list dynamic keys with cardinality ≤ threshold")
	cmd.Flags().StringVar(&policyFilePath, "policy-file", "", "JSON policy override (re-classifies in-memory)")
	return cmd
}

func runTags(inPath, byKey string, showPromotable bool, policyPath string, out io.Writer) error {
	doc, err := loadClustersDoc(inPath)
	if err != nil {
		return err
	}
	policy, err := loadPolicy(policyPath)
	if err != nil {
		return err
	}
	if policyPath != "" {
		reclassify(doc.Clusters, policy)
	}
	switch {
	case showPromotable:
		return emitPromotable(out, doc.Clusters, policy.CardinalityPromoteThreshold)
	case byKey != "":
		return emitGroupBy(out, doc.Clusters, byKey)
	default:
		return emitSummary(out, doc.Clusters)
	}
}

func reclassify(clusters []qshape.Cluster, policy *tags.Policy) {
	for i := range clusters {
		c := &clusters[i]
		if len(c.Members) == 0 {
			continue
		}
		extracted := tags.Extract(c.Members[0].Raw)
		if len(extracted) == 0 {
			c.Owners, c.RegresqlMeta, c.DynamicTagKeys = nil, nil, nil
			continue
		}
		cl := tags.Classify(extracted, policy)
		c.Owners = cl.Owners
		c.RegresqlMeta = cl.RegresqlMeta
		c.DynamicTagKeys = nil
		for _, d := range cl.DynamicKeys {
			c.DynamicTagKeys = append(c.DynamicTagKeys, qshape.DynamicKeyObservation{
				Key: d.Key, ValueCardinalitySeen: d.ValueCardinalitySeen,
			})
		}
		sort.Slice(c.DynamicTagKeys, func(i, j int) bool {
			return c.DynamicTagKeys[i].Key < c.DynamicTagKeys[j].Key
		})
	}
}

func emitSummary(out io.Writer, clusters []qshape.Cluster) error {
	tagged, untagged := 0, 0
	for _, c := range clusters {
		if len(c.Owners) > 0 || len(c.RegresqlMeta) > 0 {
			tagged++
		} else {
			untagged++
		}
	}
	fmt.Fprintf(out, "clusters: %d\n", len(clusters))
	fmt.Fprintf(out, "  tagged:   %d\n", tagged)
	fmt.Fprintf(out, "  untagged: %d\n", untagged)
	return nil
}

func emitGroupBy(out io.Writer, clusters []qshape.Cluster, key string) error {
	type bucket struct {
		clusters int
		calls    int64
		execMs   float64
	}
	buckets := map[string]*bucket{}
	for _, c := range clusters {
		v := c.Owners[key]
		if v == "" {
			v = "(none)"
		}
		b, ok := buckets[v]
		if !ok {
			b = &bucket{}
			buckets[v] = b
		}
		b.clusters++
		b.calls += c.TotalCalls
		b.execMs += c.TotalExecTimeMs
	}
	keys := make([]string, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return buckets[keys[i]].calls > buckets[keys[j]].calls
	})
	fmt.Fprintf(out, "%-40s %10s %10s %15s\n", key, "clusters", "calls", "exec_ms")
	for _, k := range keys {
		b := buckets[k]
		fmt.Fprintf(out, "%-40s %10d %10d %15.2f\n", k, b.clusters, b.calls, b.execMs)
	}
	return nil
}

func emitPromotable(out io.Writer, clusters []qshape.Cluster, threshold int) error {
	type row struct {
		key         string
		cardinality int
		calls       int64
		clusters    int
	}
	agg := map[string]*row{}
	for _, c := range clusters {
		for _, d := range c.DynamicTagKeys {
			r, ok := agg[d.Key]
			if !ok {
				r = &row{key: d.Key}
				agg[d.Key] = r
			}
			r.cardinality += d.ValueCardinalitySeen
			r.calls += c.TotalCalls
			r.clusters++
		}
	}
	out2 := make([]*row, 0, len(agg))
	for _, r := range agg {
		if r.cardinality <= threshold {
			out2 = append(out2, r)
		}
	}
	sort.Slice(out2, func(i, j int) bool { return out2[i].calls > out2[j].calls })
	fmt.Fprintf(out, "promotable dynamic keys (cardinality ≤ %d):\n", threshold)
	fmt.Fprintf(out, "%-30s %12s %10s %10s\n", "key", "cardinality", "calls", "clusters")
	for _, r := range out2 {
		fmt.Fprintf(out, "%-30s %12d %10d %10d\n", r.key, r.cardinality, r.calls, r.clusters)
	}
	return nil
}

