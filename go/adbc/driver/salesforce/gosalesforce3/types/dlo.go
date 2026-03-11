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

// CreateDataLakeObjectRequest is the request body for creating a DLO.
// Note: field names differ from the response (DataLakeObject).
type CreateDataLakeObjectRequest struct {
	Name                       string                           `json:"name"`
	Label                      string                           `json:"label"`
	Category                   DataLakeObjectCategory           `json:"category"`
	Fields                     []DataLakeFieldInputRepresentation `json:"dataLakeFieldInputRepresentations"`
	DataspaceInfo              []DataspaceInfo                  `json:"dataspaceInfo"`
	OrgUnitIdentifierFieldName string                           `json:"orgUnitIdentifierFieldName"`
	RecordModifiedFieldName    string                           `json:"recordModifiedFieldName"`
	EventDateTimeFieldName     string                           `json:"eventDateTimeFieldName,omitempty"`
}

// DataLakeFieldInputRepresentation is a field in the DLO create request.
// Note: IsPrimaryKey is a string ("true"/"false") per the Salesforce API.
type DataLakeFieldInputRepresentation struct {
	Name         string `json:"name"`
	Label        string `json:"label"`
	DataType     string `json:"dataType"`
	IsPrimaryKey string `json:"isPrimaryKey"`
}

// DataLakeObjects is the wrapper response from the GET endpoint.
type DataLakeObjects struct {
	DataLakeObjects []DataLakeObject `json:"dataLakeObjects"`
}

// DataLakeObject is the response representation of a DLO.
type DataLakeObject struct {
	ID       string                 `json:"id,omitempty"`
	Name     string                 `json:"name"`
	Category DataLakeObjectCategory `json:"category"`
	Status   DataLakeObjectStatus   `json:"publishStatus,omitempty"`
	Fields   []DataLakeFieldOutput  `json:"dataLakeFieldInfoRepresentation,omitempty"`
}

func (d *DataLakeObject) IsActive() bool {
	return strings.EqualFold(d.Status, DLOStatusActive)
}

func (d *DataLakeObject) IsError() bool {
	return strings.EqualFold(d.Status, DLOStatusError)
}

// DataLakeFieldOutput is a field in the DLO response.
type DataLakeFieldOutput struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName"`
	Type         string `json:"type"`
	IsPrimaryKey bool   `json:"isPrimaryKey"`
}

type DataspaceInfo struct {
	Name string `json:"name"`
}
