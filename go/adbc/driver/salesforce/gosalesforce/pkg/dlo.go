package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// PostDataLakeObject creates a new Data Lake Object (DLO) in Data Cloud
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-data-lake-objects.html
func (c *Client) PostDataLakeObject(ctx context.Context, request *CreateDataLakeObjectRequest) (*DataLakeObjectResponse, error) {
	if request == nil {
		return nil, &AuthError{
			Code:    400,
			Message: "Data Lake Object request cannot be nil",
			Type:    "invalid_request",
		}
	}

	if request.Name == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "Data Lake Object name cannot be empty",
			Type:    "invalid_request",
		}
	}

	if request.Label == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "Data Lake Object label cannot be empty",
			Type:    "invalid_request",
		}
	}

	// Prepare the request body
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data lake object request: %w", err)
	}

	dloURL := c.buildServicesURL(c.accessToken.InstanceURL, "data-lake-objects")

	req, err := http.NewRequestWithContext(ctx, "POST", dloURL, strings.NewReader(string(requestBody)))
	if err != nil {
		return nil, fmt.Errorf("failed to create data lake object request: %w", err)
	}

	setCommonHeaders(req, c.accessToken.AccessToken)

	// Execute request with retries
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read data lake object creation response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, handleErrorResponse(resp.StatusCode, body, "data_lake_object_creation_failed")
	}

	var dloResponse DataLakeObjectResponse
	if err := json.Unmarshal(body, &dloResponse); err != nil {
		return nil, fmt.Errorf("failed to parse data lake object creation response: %w", err)
	}

	return &dloResponse, nil
}

// NewDataLakeObjectRequest creates a new Data Lake Object request with basic fields
func NewDataLakeObjectRequest(name, label string, category DataLakeObjectCategory, fields []DataLakeFieldInputRepresentation) *CreateDataLakeObjectRequest {
	return &CreateDataLakeObjectRequest{
		Name:                              name,
		Label:                             label,
		Category:                          category,
		DataspaceInfo:                     []DataspaceInfo{},
		OrgUnitIdentifierFieldName:        "",
		RecordModifiedFieldName:           "",
		DataLakeFieldInputRepresentations: fields,
	}
}

// NewDataLakeField creates a new Data Lake field representation
func NewDataLakeField(name, label string, dataType DataLakeFieldDataType, isPrimaryKey bool) DataLakeFieldInputRepresentation {
	primaryKeyStr := "false"
	if isPrimaryKey {
		primaryKeyStr = "true"
	}

	return DataLakeFieldInputRepresentation{
		Name:         name,
		Label:        label,
		DataType:     dataType,
		IsPrimaryKey: primaryKeyStr,
	}
}

// NewDataspaceInfo creates a new dataspace info with filter configuration
func NewDataspaceInfo(name string, conditions []FilterCondition, operator ConjunctiveOperator) DataspaceInfo {
	return DataspaceInfo{
		Name: name,
		Filter: FilterConfig{
			ConjunctiveOperator: operator,
			Conditions: FilterConditions{
				Conditions: conditions,
			},
		},
	}
}

// NewFilterCondition creates a new filter condition
func NewFilterCondition(fieldName, filterValue, tableName string, operator FilterOperator) FilterCondition {
	return FilterCondition{
		FieldName:   fieldName,
		FilterValue: filterValue,
		Operator:    operator,
		TableName:   tableName,
	}
}

// NewProfileDataLakeObject creates a new Profile category Data Lake Object request
func NewProfileDataLakeObject(name, label string, fields []DataLakeFieldInputRepresentation) *CreateDataLakeObjectRequest {
	return NewDataLakeObjectRequest(name, label, DataLakeObjectCategoryProfile, fields)
}

// NewEngagementDataLakeObject creates a new Engagement category Data Lake Object request
func NewEngagementDataLakeObject(name, label, eventDateTimeFieldName string, fields []DataLakeFieldInputRepresentation) *CreateDataLakeObjectRequest {
	request := NewDataLakeObjectRequest(name, label, DataLakeObjectCategoryEngagement, fields)
	request.EventDateTimeFieldName = eventDateTimeFieldName
	return request
}
