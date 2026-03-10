package types

import "strings"

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

type DataTransform struct {
	ID            string                  `json:"id,omitempty"`
	Name          string                  `json:"name"`
	Label         string                  `json:"label,omitempty"`
	Status        DataTransformStatus     `json:"publishStatus"`
	LastRunStatus DataTransformRunStatus  `json:"lastRunStatus,omitempty"`
	Definition    DataTransformDefinition `json:"definition,omitempty"`
}

func (dt *DataTransform) IsActive() bool         { return dt.Status.IsActive() }
func (dt *DataTransform) IsLastRunSuccess() bool  { return dt.LastRunStatus.IsSuccess() }
func (dt *DataTransform) IsLastRunFailure() bool  { return dt.LastRunStatus.IsFailure() }
func (dt *DataTransform) IsLastRunCanceled() bool { return dt.LastRunStatus.IsCanceled() }

type DataTransformDefinition struct {
	Type     string                         `json:"type"`
	Version  string                         `json:"version"`
	Manifest DataTransformManifest          `json:"manifest,omitempty"`
	Nodes    map[string]DataTransformNode   `json:"nodes,omitempty"`
	Sources  map[string]DataTransformSource `json:"sources,omitempty"`
}

type DataTransformManifest struct {
	OutputObjects []DataTransformOutput `json:"outputObjects,omitempty"`
}

type DataTransformOutput struct {
	ObjectName string `json:"objectName"`
	Label      string `json:"label,omitempty"`
	Category   string `json:"category,omitempty"`
}

type DataTransformNode struct {
	SQL     string   `json:"sql"`
	Sources []string `json:"sources,omitempty"`
}

type DataTransformSource struct {
	ObjectName string `json:"objectName"`
}

type CreateDataTransformRequest struct {
	Name       string                  `json:"name"`
	Label      string                  `json:"label,omitempty"`
	Definition DataTransformDefinition `json:"definition"`
}

type DataTransformValidation struct {
	Valid  bool     `json:"valid"`
	Errors []string `json:"errors,omitempty"`
}
