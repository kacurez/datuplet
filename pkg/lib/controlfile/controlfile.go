// Package controlfile provides helpers for reading and writing the
// _datuplet_tableinfo.json control file that persists partition spec
// between DataGateway (reader) and TableCommit (writer).
package controlfile

import (
	"strings"
)

const controlFileName = "_datuplet_tableinfo.json"

// PartitionField describes a single partition field in the stable format.
type PartitionField struct {
	SourceColumn string `json:"source_column"`
	Transform    string `json:"transform"`
	FieldName    string `json:"field_name"` // Directory name used in partition paths
}

// TableInfo is the control file content persisted in metadata/.
type TableInfo struct {
	PartitionSpec  []PartitionField `json:"partition_spec"`
	SchemaFields   []string         `json:"schema_fields"`
	LastSnapshotID int64            `json:"last_snapshot_id"`
}

// ControlFilePath returns the full storage path for the control file
// given a table base path (e.g., s3://bucket/orgs/.../tables/raw/orders/).
func ControlFilePath(tableBasePath string) string {
	if !strings.HasSuffix(tableBasePath, "/") {
		tableBasePath += "/"
	}
	return tableBasePath + "metadata/" + controlFileName
}

// DeriveFieldName derives the partition directory name from source column and transform.
// identity → same as source_column
// day/month/year/hour → {source_column}_{transform}
func DeriveFieldName(sourceColumn, transform string) string {
	if transform == "identity" {
		return sourceColumn
	}
	return sourceColumn + "_" + transform
}
