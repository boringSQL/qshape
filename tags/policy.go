package tags

import (
	"regexp"
	"strings"
)

const maxValueLen = 256

var (
	reEmail    = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)
	reJWT      = regexp.MustCompile(`^[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}$`)
	reLongB64  = regexp.MustCompile(`^[A-Za-z0-9+/_=-]{64,}$`)
)

func DefaultPolicy() *Policy {
	return &Policy{
		Stable: stringSet(
			"application", "controller", "action", "route",
			"framework", "job", "db_driver", "release", "environment",
		),
		Deny: stringSet(
			"traceparent", "tracestate", "trace_id", "span_id",
			"request_id", "tenant_id", "user_id", "session_id",
		),
		VendorMap: map[string]string{
			"dd.service":                     "application",
			"nr.application":                 "application",
			"otel.service.name":              "application",
			"dd.env":                         "environment",
			"otel.deployment.environment":    "environment",
			"dd.version":                     "release",
			"otel.service.version":           "release",
			"dd.trace_id":                    "trace_id",
			"nr.trace_id":                    "trace_id",
			"otel.trace_id":                  "trace_id",
		},
		Reserved: stringSet(
			"name", "description", "max-cost", "required-nodes", "timeout", "cardinality",
		),
		CardinalityPromoteThreshold: 100,
	}
}

func Classify(tags []Tag, p *Policy) ClassifiedTags {
	if p == nil {
		p = DefaultPolicy()
	}
	out := ClassifiedTags{}
	seenDyn := map[string]int{}
	for _, t := range tags {
		k := t.Key
		if mapped, ok := p.VendorMap[k]; ok {
			k = mapped
		}
		if _, denied := p.Deny[k]; denied {
			seenDyn[k]++
			continue
		}
		if _, reserved := p.Reserved[k]; reserved {
			if out.RegresqlMeta == nil {
				out.RegresqlMeta = map[string]string{}
			}
			out.RegresqlMeta[k] = t.Value
			continue
		}
		if _, stable := p.Stable[k]; stable {
			if isUnsafeValue(t.Value) {
				seenDyn[k]++
				continue
			}
			if out.Owners == nil {
				out.Owners = map[string]string{}
			}
			out.Owners[k] = t.Value
			continue
		}
		seenDyn[k]++
	}
	if len(seenDyn) > 0 {
		out.DynamicKeys = make([]DynamicKeyObservation, 0, len(seenDyn))
		for k, n := range seenDyn {
			out.DynamicKeys = append(out.DynamicKeys, DynamicKeyObservation{Key: k, ValueCardinalitySeen: n})
		}
	}
	return out
}

func isUnsafeValue(v string) bool {
	if len(v) > maxValueLen {
		return true
	}
	if reEmail.MatchString(v) {
		return true
	}
	if reJWT.MatchString(v) {
		return true
	}
	if strings.Count(v, ".") == 0 && reLongB64.MatchString(v) {
		return true
	}
	return false
}

func stringSet(keys ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}
