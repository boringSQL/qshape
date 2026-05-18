package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/boringsql/qshape/tags"
)

type policyFile struct {
	Stable                      []string          `json:"stable"`
	Deny                        []string          `json:"deny"`
	Reserved                    []string          `json:"reserved"`
	VendorMap                   map[string]string `json:"vendor_map"`
	CardinalityPromoteThreshold int               `json:"cardinality_promote_threshold"`
}

func loadPolicy(path string) (*tags.Policy, error) {
	p := tags.DefaultPolicy()
	if path == "" {
		return p, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open policy file: %w", err)
	}
	defer f.Close()
	var pf policyFile
	if err := json.NewDecoder(f).Decode(&pf); err != nil {
		return nil, fmt.Errorf("decode policy file: %w", err)
	}
	if len(pf.Stable) > 0 {
		p.Stable = setFromSlice(pf.Stable)
	}
	if len(pf.Deny) > 0 {
		p.Deny = setFromSlice(pf.Deny)
	}
	if len(pf.Reserved) > 0 {
		p.Reserved = setFromSlice(pf.Reserved)
	}
	if len(pf.VendorMap) > 0 {
		p.VendorMap = pf.VendorMap
	}
	if pf.CardinalityPromoteThreshold > 0 {
		p.CardinalityPromoteThreshold = pf.CardinalityPromoteThreshold
	}
	return p, nil
}

func setFromSlice(s []string) map[string]struct{} {
	m := make(map[string]struct{}, len(s))
	for _, k := range s {
		m[k] = struct{}{}
	}
	return m
}
