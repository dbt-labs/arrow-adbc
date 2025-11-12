// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sparklivy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// SparkSchema represents a Spark schema JSON structure
type SparkSchema struct {
	Type   string       `json:"type"`
	Fields []SparkField `json:"fields"`
}

// SparkField represents a field in a Spark schema
type SparkField struct {
	Name     string                 `json:"name"`
	Type     interface{}            `json:"type"` // Can be string or nested struct
	Nullable bool                   `json:"nullable"`
	Metadata map[string]interface{} `json:"metadata"`
}

// parseSparkSchemaJSON parses a Spark schema JSON string and converts it to Arrow schema
func parseSparkSchemaJSON(schemaJSON string) (*arrow.Schema, error) {
	// Clean up the JSON if it's wrapped in quotes or has extra whitespace
	schemaJSON = strings.TrimSpace(schemaJSON)

	// Handle Scala REPL output format: "res0: String = {...}"
	// Extract just the JSON part after the equals sign
	if idx := strings.Index(schemaJSON, " = "); idx != -1 {
		schemaJSON = schemaJSON[idx+3:]
		schemaJSON = strings.TrimSpace(schemaJSON)
	}

	schemaJSON = strings.Trim(schemaJSON, "\"")
	schemaJSON = strings.ReplaceAll(schemaJSON, "\\\"", "\"")

	var sparkSchema SparkSchema
	if err := json.Unmarshal([]byte(schemaJSON), &sparkSchema); err != nil {
		return nil, fmt.Errorf("failed to parse Spark schema JSON: %w", err)
	}

	if sparkSchema.Type != "struct" {
		return nil, fmt.Errorf("expected struct type at root, got: %s", sparkSchema.Type)
	}

	fields := make([]arrow.Field, len(sparkSchema.Fields))
	for i, sparkField := range sparkSchema.Fields {
		arrowField, err := convertSparkFieldToArrow(sparkField)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", sparkField.Name, err)
		}
		fields[i] = arrowField
	}

	return arrow.NewSchema(fields, nil), nil
}

// convertSparkFieldToArrow converts a Spark field to an Arrow field
func convertSparkFieldToArrow(sparkField SparkField) (arrow.Field, error) {
	arrowType, err := convertSparkTypeToArrow(sparkField.Type)
	if err != nil {
		return arrow.Field{}, err
	}

	return arrow.Field{
		Name:     sparkField.Name,
		Type:     arrowType,
		Nullable: sparkField.Nullable,
	}, nil
}

// convertSparkTypeToArrow converts a Spark type to an Arrow type
func convertSparkTypeToArrow(sparkType interface{}) (arrow.DataType, error) {
	switch t := sparkType.(type) {
	case string:
		// Simple type
		return convertSimpleSparkType(t)
	case map[string]interface{}:
		// Complex type (struct, array, map)
		return convertComplexSparkType(t)
	default:
		return nil, fmt.Errorf("unsupported Spark type format: %T", sparkType)
	}
}

// convertSimpleSparkType converts a simple Spark type string to Arrow type
func convertSimpleSparkType(sparkType string) (arrow.DataType, error) {
	switch strings.ToLower(sparkType) {
	case "string":
		return arrow.BinaryTypes.String, nil
	case "byte", "tinyint":
		return arrow.PrimitiveTypes.Int8, nil
	case "short", "smallint":
		return arrow.PrimitiveTypes.Int16, nil
	case "int", "integer":
		return arrow.PrimitiveTypes.Int32, nil
	case "long", "bigint":
		return arrow.PrimitiveTypes.Int64, nil
	case "float":
		return arrow.PrimitiveTypes.Float32, nil
	case "double":
		return arrow.PrimitiveTypes.Float64, nil
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nil
	case "timestamp":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	default:
		// Handle decimal, etc.
		if strings.HasPrefix(sparkType, "decimal(") {
			// Parse decimal(precision, scale)
			// For now, default to decimal128
			return arrow.PrimitiveTypes.Float64, nil // Fallback to float64
		}
		return nil, fmt.Errorf("unsupported Spark type: %s", sparkType)
	}
}

// convertComplexSparkType converts a complex Spark type to Arrow type
func convertComplexSparkType(typeMap map[string]interface{}) (arrow.DataType, error) {
	typeStr, ok := typeMap["type"].(string)
	if !ok {
		return nil, fmt.Errorf("complex type missing 'type' field")
	}

	switch typeStr {
	case "array":
		// Array type
		elementType, ok := typeMap["elementType"]
		if !ok {
			return nil, fmt.Errorf("array type missing 'elementType'")
		}
		elementArrowType, err := convertSparkTypeToArrow(elementType)
		if err != nil {
			return nil, err
		}
		// Note: containsNull handling may need refinement in the future
		_ = typeMap["containsNull"] // Acknowledge the field exists
		return arrow.ListOf(elementArrowType), nil

	case "map":
		// Map type
		keyType, ok := typeMap["keyType"]
		if !ok {
			return nil, fmt.Errorf("map type missing 'keyType'")
		}
		valueType, ok := typeMap["valueType"]
		if !ok {
			return nil, fmt.Errorf("map type missing 'valueType'")
		}

		keyArrowType, err := convertSparkTypeToArrow(keyType)
		if err != nil {
			return nil, err
		}
		valueArrowType, err := convertSparkTypeToArrow(valueType)
		if err != nil {
			return nil, err
		}

		return arrow.MapOf(keyArrowType, valueArrowType), nil

	case "struct":
		// Struct type
		fields, ok := typeMap["fields"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("struct type missing or invalid 'fields'")
		}

		arrowFields := make([]arrow.Field, len(fields))
		for i, fieldInterface := range fields {
			fieldMap, ok := fieldInterface.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("invalid field format in struct")
			}

			// Parse the field
			name, _ := fieldMap["name"].(string)
			fieldType := fieldMap["type"]
			nullable, _ := fieldMap["nullable"].(bool)

			arrowFieldType, err := convertSparkTypeToArrow(fieldType)
			if err != nil {
				return nil, fmt.Errorf("failed to convert struct field type: %w", err)
			}

			arrowFields[i] = arrow.Field{
				Name:     name,
				Type:     arrowFieldType,
				Nullable: nullable,
			}
		}

		return arrow.StructOf(arrowFields...), nil

	default:
		return nil, fmt.Errorf("unsupported complex Spark type: %s", typeStr)
	}
}
