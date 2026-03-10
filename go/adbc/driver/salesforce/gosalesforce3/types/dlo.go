package types

import "strings"

type DataLakeObjectCategory = string

const (
	DLOCategoryProfile    DataLakeObjectCategory = "Profile"
	DLOCategoryEngagement DataLakeObjectCategory = "Engagement"
	DLOCategoryOther      DataLakeObjectCategory = "Other"
)

type DataLakeObjectStatus = string

const (
	DLOStatusActive     DataLakeObjectStatus = "Active"
	DLOStatusProcessing DataLakeObjectStatus = "Processing"
	DLOStatusError      DataLakeObjectStatus = "Error"
)

type DataLakeObject struct {
	ID       string                 `json:"id,omitempty"`
	Name     string                 `json:"objectName"`
	Label    string                 `json:"label"`
	Category DataLakeObjectCategory `json:"category"`
	Status   DataLakeObjectStatus   `json:"publishStatus,omitempty"`
	Fields   []DataLakeField        `json:"fields,omitempty"`

	OrgUnitIdentifierFieldName string          `json:"orgUnitIdentifierFieldName,omitempty"`
	RecordModifiedFieldName    string          `json:"recordModifiedFieldName,omitempty"`
	DataspaceInfo              []DataspaceInfo `json:"dataspaceInfo,omitempty"`
}

func (d *DataLakeObject) IsActive() bool {
	return strings.EqualFold(d.Status, DLOStatusActive)
}

func (d *DataLakeObject) IsError() bool {
	return strings.EqualFold(d.Status, DLOStatusError)
}

type DataLakeField struct {
	Name         string `json:"name"`
	Label        string `json:"label,omitempty"`
	Type         string `json:"type"`
	IsPrimaryKey bool   `json:"isPrimaryKey"`
}

type DataspaceInfo struct {
	Name string `json:"name"`
}

type CreateDataLakeObjectRequest = DataLakeObject
