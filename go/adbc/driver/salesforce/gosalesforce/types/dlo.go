package types

import "strings"

type DataLakeObjectStatus = string

const (
	DLOStatusActive     DataLakeObjectStatus = "Active"
	DLOStatusProcessing DataLakeObjectStatus = "Processing"
	DLOStatusError      DataLakeObjectStatus = "Error"
)

// DataLakeObjectRequest is the request body for creating a DLO.
//
// The API has two field representations: `fields` and `dataLakeFieldInputRepresentations`.
// `fields` appears in responses with a `type` enum and `keyQualifierFieldName`.
// `dataLakeFieldInputRepresentations` appears in create requests with a `dataType` enum
// (partially overlapping variants). The exact distinction is unclear — pending Salesforce clarification.
type DataLakeObjectRequest struct {
	Name                       string                             `json:"name"`
	Label                      string                             `json:"label"`
	Category                   DataObjectCategory                 `json:"category"`
	FieldInputRepresentations  []DataLakeFieldInputRepresentation `json:"dataLakeFieldInputRepresentations"`
	DataspaceInfo              []DataspaceInfo                    `json:"dataspaceInfo"`
	OrgUnitIdentifierFieldName string                             `json:"orgUnitIdentifierFieldName"`
	RecordModifiedFieldName    string                             `json:"recordModifiedFieldName"`
	EventDateTimeFieldName     string                             `json:"eventDateTimeFieldName,omitempty"`
}

// DataLakeFieldInputRepresentation is a field in the DLO create request.
// Note: IsPrimaryKey is a string ("true"/"false") per the Salesforce API.
type DataLakeFieldInputRepresentation struct {
	Name         string `json:"name"`
	Label        string `json:"label"`
	DataType     string `json:"dataType"`
	IsPrimaryKey bool   `json:"isPrimaryKey"`
}

// DataLakeObjects is the wrapper response from the GET endpoint.
type DataLakeObjects struct {
	DataLakeObjects []DataLakeObject `json:"dataLakeObjects"`
}

// DataLakeObject is the response representation of a DLO.
type DataLakeObject struct {
	ID       string                `json:"id,omitempty"`
	Name     string                `json:"name"`
	Category DataObjectCategory    `json:"category"`
	Status   DataLakeObjectStatus  `json:"status,omitempty"`
	Fields   []DataLakeFieldOutput `json:"dataLakeFieldInfoRepresentation,omitempty"`
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
