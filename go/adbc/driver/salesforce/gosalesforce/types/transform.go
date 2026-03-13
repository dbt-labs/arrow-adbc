package types

import "strings"

// DataTransformType represents the type of data transform.
type DataTransformType = string

const (
	DataTransformTypeBatch DataTransformType = "BATCH"
)

// DataTransformDefinitionType represents the type of definition.
type DataTransformDefinitionType = string

const (
	DataTransformDefinitionTypeDCSQL DataTransformDefinitionType = "DCSQL"
)

type DataTransformStatus string

const (
	TransformStatusActive     DataTransformStatus = "Active"
	TransformStatusError      DataTransformStatus = "Error"
	TransformStatusProcessing DataTransformStatus = "Processing"
	TransformStatusDeleting   DataTransformStatus = "Deleting"
)

func (s DataTransformStatus) IsActive() bool {
	return strings.EqualFold(string(s), string(TransformStatusActive))
}
func (s DataTransformStatus) IsError() bool {
	return strings.EqualFold(string(s), string(TransformStatusError))
}
func (s DataTransformStatus) IsProcessing() bool {
	return strings.EqualFold(string(s), string(TransformStatusProcessing))
}

type DataTransformRunStatus string

const (
	RunStatusSuccess    DataTransformRunStatus = "Success"
	RunStatusFailure    DataTransformRunStatus = "Failure"
	RunStatusCanceled   DataTransformRunStatus = "Canceled"
	RunStatusPending    DataTransformRunStatus = "Pending"
	RunStatusInProgress DataTransformRunStatus = "InProgress"
)

func (s DataTransformRunStatus) IsSuccess() bool {
	return strings.EqualFold(string(s), string(RunStatusSuccess))
}
func (s DataTransformRunStatus) IsFailure() bool {
	return strings.EqualFold(string(s), string(RunStatusFailure))
}
func (s DataTransformRunStatus) IsCanceled() bool {
	return strings.EqualFold(string(s), string(RunStatusCanceled))
}
func (s DataTransformRunStatus) IsPending() bool {
	return strings.EqualFold(string(s), string(RunStatusPending))
}
func (s DataTransformRunStatus) IsInProgress() bool {
	return strings.EqualFold(string(s), string(RunStatusInProgress))
}
func (s DataTransformRunStatus) IsTerminal() bool {
	return s.IsSuccess() || s.IsFailure() || s.IsCanceled()
}

// OutputDataObjectType is the discriminant for output data objects.
type OutputDataObjectType = string

const (
	OutputDataObjectTypeDLO OutputDataObjectType = "dataLakeObject"
	OutputDataObjectTypeDMO OutputDataObjectType = "dataModelObject"
)

// Materialization controls how a transform node materializes its output.
type Materialization = string

const (
	MaterializationTable     Materialization = "table"
	MaterializationEphemeral Materialization = "ephemeral"
)

// WriteMode controls how a materialized table is written.
// Only relevant when Materialization is "table".
type WriteMode = string

const (
	WriteModeOverwrite WriteMode = "OVERWRITE"
	WriteModeAppend    WriteMode = "APPEND"
	WriteModeMerge     WriteMode = "MERGE"
)

// ValidationSeverity is the severity of a validation issue.
type ValidationSeverity = string

const (
	ValidationSeverityWarning ValidationSeverity = "WARNING"
	ValidationSeverityError   ValidationSeverity = "ERROR"
	ValidationSeverityFatal   ValidationSeverity = "FATAL"
)

// DataTransformNodeID uniquely identifies a node within a transform manifest.
// Used as map keys and in depends_on references to establish the dependency graph.
type DataTransformNodeID string

// DataTransform represents a Data Transform resource (response).
type DataTransform struct {
	ID               string                  `json:"id,omitempty"`
	Name             string                  `json:"name"`
	Label            string                  `json:"label,omitempty"`
	Status           DataTransformStatus     `json:"status"`
	LastRunStatus    DataTransformRunStatus  `json:"lastRunStatus,omitempty"`
	LastRunErrorCode any                     `json:"lastRunErrorCode,omitempty"` // TODO: proper struct type needed
	LastRunDate      string                  `json:"lastRunDate,omitempty"`
	Definition       DataTransformDefinition `json:"definition,omitzero"`
	Type             DataTransformType       `json:"type,omitempty"`
}

func (dt *DataTransform) IsActive() bool          { return dt.Status.IsActive() }
func (dt *DataTransform) IsLastRunSuccess() bool  { return dt.LastRunStatus.IsSuccess() }
func (dt *DataTransform) IsLastRunFailure() bool  { return dt.LastRunStatus.IsFailure() }
func (dt *DataTransform) IsLastRunCanceled() bool { return dt.LastRunStatus.IsCanceled() }

// DataTransformDefinition holds the transform logic.
type DataTransformDefinition struct {
	Type              DataTransformDefinitionType    `json:"type"`
	Version           string                         `json:"version"`
	Manifest          DataTransformManifest          `json:"manifest,omitzero"`
	OutputDataObjects DataTransformOutputDataObjects `json:"outputDataObjects,omitempty"`
}

// DataTransformManifest is the dbt-style manifest with nodes.
type DataTransformManifest struct {
	Nodes DataTransformNodes `json:"nodes,omitempty"`
}

// DataTransformNodes maps node IDs to their definitions.
// Node IDs are arbitrary identifiers used to establish the dependency graph.
// They don't need to match the node's Name field.
type DataTransformNodes map[DataTransformNodeID]DataTransformNode

// DataTransformNode represents a computation node in a transform.
//
// Nodes are analogous to CTEs in SQL:
//   - Name is the CTE alias, used to reference this node in downstream SQL
//   - RelationName is the target data object (DLO/DMO) that results are written to
//   - DependsOn explicitly declares edges in the dependency graph (unlike SQL CTEs)
type DataTransformNode struct {
	Name         string                  `json:"name"`
	RelationName string                  `json:"relation_name,omitempty"`
	Config       DataTransformNodeConfig `json:"config"`
	CompiledCode string                  `json:"compiled_code"`
	DependsOn    DataTransformDependsOn  `json:"depends_on,omitzero"`
}

// DataTransformDependsOn declares the dependency edges for a node.
type DataTransformDependsOn struct {
	Nodes []DataTransformNodeID `json:"nodes,omitempty"`
}

// DataTransformNodeConfig configures how a node materializes.
type DataTransformNodeConfig struct {
	Materialized Materialization `json:"materialized"`
	WriteMode    WriteMode       `json:"writeMode,omitempty"`
}

// DataTransformOutputDataObject describes an output object of a transform.
// It is the base shape for both DataLakeObject and DataModelObject outputs,
// discriminated by the Type field.
type DataTransformOutputDataObject struct {
	Type      OutputDataObjectType `json:"type"`
	Name      string               `json:"name"`
	Label     string               `json:"label,omitempty"`
	Category  DataObjectCategory   `json:"category,omitempty"`
	Namespace string               `json:"namespace,omitempty"`
	Fields    []DataTransformOutputField `json:"fields,omitempty"`
}

// DataTransformOutputField describes a field in an output data object.
type DataTransformOutputField struct {
	Name              string `json:"name"`
	Label             string `json:"label,omitempty"`
	Type              string `json:"type"`
	IsPrimaryKey      bool   `json:"isPrimaryKey"`
	KeyQualifierField string `json:"keyQualifierField,omitempty"`
}

// DataTransformOutputDataObjects is a slice of output data objects.
type DataTransformOutputDataObjects []DataTransformOutputDataObject

// DataTransformRequest is the request body for creating a Data Transform.
type DataTransformRequest struct {
	Name          string                  `json:"name"`
	Label         string                  `json:"label,omitempty"`
	Type          DataTransformType       `json:"type"`
	Definition    DataTransformDefinition `json:"definition"`
	DataSpaceName string                  `json:"dataSpaceName,omitempty"`
	Description   string                  `json:"description,omitempty"`
	PrimarySource string                  `json:"primarySource,omitempty"`
}

// DataTransformValidation is the response from the validation endpoint.
type DataTransformValidation struct {
	// Note: Just because the Issue.Code is "ERROR" doesn't mean the transform is invalid.
	// For example, validating a transform that uses auto-create/update DLO will return ERROR issues about the DLO not existing.
	// We've been told by Salesforce that these issues can be ignored, and are expected to be present.
	//
	// Note: There seem to be two "kinds" of issue:
	// - Per Transform
	// - Per OutputDataObject Field/Column
	// The API doesn't distingush between these, but can be a bit confusing since we'll see "duplicate" issues in some cases.
	// For example, if a transform doesn't exist, we'll get an `ERROR:TARGET_DLO_NOT_FOUND` issue for each column in the output schema.
	Issues []DataTransformValidationIssue `json:"issues,omitempty"`
	// Note: The API returns a map keyed by transform name, but AFAIK there is only ever one key.
	OutputDataObjects map[string]DataTransformOutputDataObjects `json:"outputDataObjects,omitempty"`
}

// DataTransformValidationIssue represents a validation problem.
type DataTransformValidationIssue struct {
	Code     string             `json:"errorCode"`
	Message  string             `json:"errorMessage"`
	Severity ValidationSeverity `json:"errorSeverity"`
}
