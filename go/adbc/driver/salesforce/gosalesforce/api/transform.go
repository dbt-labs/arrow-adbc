package api

import (
	"context"
)

// CreateDataTransform creates a new data transform in Data Cloud
// reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=createDataTransform
func (c *Client) CreateDataTransform(ctx context.Context, request *CreateDataTransformRequest) (*DataTransformResponse, error) {
	// Validate required fields
	if request.Name == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "Data transform name cannot be empty",
			Type:    "invalid_request",
		}
	}

	if request.Label == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "Data transform label cannot be empty",
			Type:    "invalid_request",
		}
	}

	return PostJSON[CreateDataTransformRequest, DataTransformResponse](c, ctx, "data-transforms", request)
}

// NewBatchDataTransformRequest creates a new batch data transform request with dbt-style definition
func NewBatchDataTransformRequest(name, label string, nodes map[string]DbtDataTransformNode) *CreateDataTransformRequest {
	return &CreateDataTransformRequest{
		Name:  name,
		Label: label,
		Type:  DataTransformTypeBatch,
		Definition: DataTransformDefinition{
			Type:    DataTransformDefinitionTypeDCSQL,
			Version: "1.0",
			Manifest: DbtDataTransformDefinition{
				Nodes: nodes,
			},
		},
	}
}

// NewDbtDataTransformNode creates a new dbt-style data transform node
func NewDbtDataTransformNode(name, relationName, compiledCode string, materialized string, dependsOn map[string]interface{}) DbtDataTransformNode {
	return DbtDataTransformNode{
		Name:         name,
		RelationName: relationName,
		Config: DbtDataTransformNodeConfig{
			Materialized: materialized,
		},
		CompiledCode: compiledCode,
		DependsOn:    dependsOn,
	}
}

// NewSimpleDbtNode creates a simple dbt node with minimal configuration
// Materialized is set to table
// DependsOn is set to empty map
func NewSimpleDbtDataTransformNode(name, relationName, sql string) DbtDataTransformNode {
	return DbtDataTransformNode{
		Name:         name,
		RelationName: relationName,
		Config: DbtDataTransformNodeConfig{
			Materialized: "table",
		},
		CompiledCode: sql,
		DependsOn:    make(map[string]interface{}),
	}
}
