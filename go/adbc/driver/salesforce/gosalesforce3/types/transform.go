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

// DataTransform represents a Data Transform resource (response).
type DataTransform struct {
	ID            string                  `json:"id,omitempty"`
	Name          string                  `json:"name"`
	Label         string                  `json:"label,omitempty"`
	Status        DataTransformStatus     `json:"status"`
	LastRunStatus DataTransformRunStatus  `json:"lastRunStatus,omitempty"`
	Definition    DataTransformDefinition `json:"definition,omitzero"`
	Type          DataTransformType       `json:"type,omitempty"`
}

func (dt *DataTransform) IsActive() bool         { return dt.Status.IsActive() }
func (dt *DataTransform) IsLastRunSuccess() bool  { return dt.LastRunStatus.IsSuccess() }
func (dt *DataTransform) IsLastRunFailure() bool  { return dt.LastRunStatus.IsFailure() }
func (dt *DataTransform) IsLastRunCanceled() bool { return dt.LastRunStatus.IsCanceled() }

// DataTransformDefinition holds the transform logic.
type DataTransformDefinition struct {
	Type              DataTransformDefinitionType      `json:"type"`
	Version           string                           `json:"version"`
	Manifest          DataTransformManifest            `json:"manifest,omitzero"`
	OutputDataObjects []DataTransformOutputDataObject  `json:"outputDataObjects,omitempty"`
}

// DataTransformManifest is the dbt-style manifest with nodes.
type DataTransformManifest struct {
	Nodes map[string]DataTransformNode `json:"nodes,omitempty"`
}

// DataTransformNode represents a computation node in a transform.
type DataTransformNode struct {
	Name         string                     `json:"name"`
	RelationName string                     `json:"relation_name,omitempty"`
	Config       DataTransformNodeConfig    `json:"config"`
	CompiledCode string                     `json:"compiled_code"`
	DependsOn    map[string]any             `json:"depends_on,omitempty"`
}

// DataTransformNodeConfig configures how a node materializes.
type DataTransformNodeConfig struct {
	Materialized string `json:"materialized"`
	WriteMode    string `json:"writeMode,omitempty"`
}

// DataTransformOutputDataObject describes an output object of a transform.
type DataTransformOutputDataObject struct {
	Type      string `json:"type"`
	Name      string `json:"name"`
	Label     string `json:"label,omitempty"`
	Category  string `json:"category,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

// CreateDataTransformRequest is the request body for creating/updating a Data Transform.
type CreateDataTransformRequest struct {
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
	Issues            []DataTransformValidationIssue             `json:"issues,omitempty"`
	OutputDataObjects map[string][]DataTransformOutputDataObject `json:"outputDataObjects,omitempty"`
}

// DataTransformValidationIssue represents a validation problem.
type DataTransformValidationIssue struct {
	ErrorCode     string `json:"errorCode"`
	ErrorMessage  string `json:"errorMessage"`
	ErrorSeverity string `json:"errorSeverity"`
}
