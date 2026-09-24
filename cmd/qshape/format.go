package main

import (
	"fmt"

	"github.com/boringsql/qshape"
)

const currentSchemaVersion = "2"

type clustersDoc struct {
	SchemaVersion            string           `json:"schema_version"`
	ObservedApplicationNames []string         `json:"observed_application_names,omitempty"`
	Clusters                 []qshape.Cluster `json:"clusters"`
}

func validateSchemaVersion(doc *clustersDoc) error {
	switch doc.SchemaVersion {
	// v1 entries are valid v2 entries: v2 only adds the optional kind/shape fields.
	case currentSchemaVersion, "1":
		return nil
	case "":
		return fmt.Errorf("clusters.json missing schema_version; must be %q", currentSchemaVersion)
	default:
		return fmt.Errorf("clusters.json schema_version=%q not supported; must be %q or \"1\"",
			doc.SchemaVersion, currentSchemaVersion)
	}
}
