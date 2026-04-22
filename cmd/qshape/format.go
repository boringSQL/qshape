package main

import (
	"fmt"

	"github.com/boringsql/qshape"
)

const currentSchemaVersion = "1"

type clustersDoc struct {
	SchemaVersion string           `json:"schema_version"`
	Clusters      []qshape.Cluster `json:"clusters"`
}

func validateSchemaVersion(doc *clustersDoc) error {
	switch doc.SchemaVersion {
	case currentSchemaVersion:
		return nil
	case "":
		return fmt.Errorf("clusters.json missing schema_version; must be %q", currentSchemaVersion)
	default:
		return fmt.Errorf("clusters.json schema_version=%q not supported; must be %q",
			doc.SchemaVersion, currentSchemaVersion)
	}
}
