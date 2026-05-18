package tags

import "testing"

// TestClassify_RoutesByCategory is the headline test for the policy
// layer. One synthetic Tag list, fed through Classify, should split
// into Owners (stable allowlist) / RegresqlMeta (reserved) /
// DynamicKeys (everything else). This is the partition the wire
// format depends on — if it breaks, downstream consumers see tags
// in the wrong fields.
func TestClassify_RoutesByCategory(t *testing.T) {
	tags := []Tag{
		{Key: "controller", Value: "Orders", Format: FormatSqlcommenter}, // → Owners
		{Key: "name", Value: "stub", Format: FormatHeader},               // → RegresqlMeta
		{Key: "traceparent", Value: "abc", Format: FormatSqlcommenter},   // → DynamicKeys (denied)
		{Key: "weird_thing", Value: "x", Format: FormatSqlcommenter},     // → DynamicKeys (unknown)
	}
	got := Classify(tags, DefaultPolicy())
	if got.Owners["controller"] != "Orders" {
		t.Errorf("Owners[controller] = %q", got.Owners["controller"])
	}
	if got.RegresqlMeta["name"] != "stub" {
		t.Errorf("RegresqlMeta[name] = %q", got.RegresqlMeta["name"])
	}
	keys := map[string]int{}
	for _, d := range got.DynamicKeys {
		keys[d.Key] = d.ValueCardinalitySeen
	}
	if keys["traceparent"] == 0 || keys["weird_thing"] == 0 {
		t.Errorf("expected traceparent and weird_thing in DynamicKeys, got %+v", got.DynamicKeys)
	}
}

// TestClassify_VendorMap confirms vendor-namespaced keys are
// rewritten BEFORE classification — `dd.service` becomes
// `application` (a stable allowlist key) rather than being routed
// to DynamicKeys as an unknown. This is the rule that lets Datadog
// / NewRelic / OTel agents work out of the box without per-vendor
// allowlist gymnastics.
func TestClassify_VendorMap(t *testing.T) {
	tags := []Tag{{Key: "dd.service", Value: "billing", Format: FormatSqlcommenter}}
	got := Classify(tags, DefaultPolicy())
	if got.Owners["application"] != "billing" {
		t.Errorf("dd.service should normalize to application, got Owners=%+v", got.Owners)
	}
}

// TestClassify_ContentFirewall is the safety net for PII-in-tags.
// Even when a key is allowlisted, certain value shapes must NEVER
// be retained: emails, JWTs, long base64 blobs, oversized strings.
// They get moved to DynamicKeys (counted, value dropped) instead.
// This guards against single-tenant leaks that pure cardinality
// limits would miss.
func TestClassify_ContentFirewall(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"email", "alice@example.com"},
		{"jwt", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"},
		{"too-long", string(make([]byte, 300))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tags := []Tag{{Key: "application", Value: c.value, Format: FormatSqlcommenter}}
			got := Classify(tags, DefaultPolicy())
			if _, ok := got.Owners["application"]; ok {
				t.Errorf("%s value should be firewalled, but Owners[application] is set: %q", c.name, c.value)
			}
			if len(got.DynamicKeys) == 0 {
				t.Errorf("%s value should be counted in DynamicKeys", c.name)
			}
		})
	}
}

// TestClassify_NilPolicy: callers passing nil get DefaultPolicy
// behavior. This is a small ergonomic guarantee — `Classify(t, nil)`
// is shorthand for `Classify(t, DefaultPolicy())`.
func TestClassify_NilPolicy(t *testing.T) {
	tags := []Tag{{Key: "controller", Value: "X"}}
	got := Classify(tags, nil)
	if got.Owners["controller"] != "X" {
		t.Errorf("nil policy should fall back to default, got %+v", got)
	}
}

// TestClassify_Empty: empty input → zero-value ClassifiedTags. All
// three maps/slices are nil (not allocated empties). This matters
// because the wire format uses `omitempty` — allocating empty maps
// would emit `"owners":{}` instead of omitting the field.
func TestClassify_Empty(t *testing.T) {
	got := Classify(nil, DefaultPolicy())
	if got.Owners != nil || got.RegresqlMeta != nil || got.DynamicKeys != nil {
		t.Errorf("empty input should yield all-nil result, got %+v", got)
	}
}
