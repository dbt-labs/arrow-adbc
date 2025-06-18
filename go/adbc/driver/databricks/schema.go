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

package databricks

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

const DbxSchemaTypeText = "type_text"

const (
	decimalTypeRegex        = `^(?:DECIMAL|DEC|NUMERIC)\((\d+)(?:,(\d+))?\)$`
	arrayTypeRegex          = `^ARRAY<(.*)>$`
	mapTypeRegex            = `^MAP<(.*)>$`
	structTypeRegex         = `^STRUCT<(.*)>$`
	intervalTypeRegex       = `^INTERVAL\s+(YEAR(?:\s+TO\s+MONTH)?|MONTH|DAY(?:\s+TO\s+(HOUR|MINUTE|SECOND))?|HOUR(?:\s+TO\s+(MINUTE|SECOND))?|MINUTE(?:\s+TO\s+SECOND)?|SECOND)$`
	intervalTypePrefixRegex = `^INTERVAL\s+(YEAR(?:\s+TO\s+MONTH)?|MONTH|DAY(?:\s+TO\s+(HOUR|MINUTE|SECOND))?|HOUR(?:\s+TO\s+(MINUTE|SECOND))?|MINUTE(?:\s+TO\s+SECOND)?|SECOND)`
)

// Basic DBX Types to Arrow Types (no extra processing needed)
// https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes
var basicTypeToArrowTypeMap = map[sql.ColumnInfoTypeName]arrow.DataType{
	// Integral numeric types (whole numbers)
	sql.ColumnInfoTypeNameByte:  arrow.PrimitiveTypes.Int8,  // 1-byte signed integer
	sql.ColumnInfoTypeNameShort: arrow.PrimitiveTypes.Int16, // 2-byte signed integer
	sql.ColumnInfoTypeNameInt:   arrow.PrimitiveTypes.Int32, // 4-byte signed integer
	sql.ColumnInfoTypeNameLong:  arrow.PrimitiveTypes.Int64, // 8-byte signed integer

	// Binary floating point types
	sql.ColumnInfoTypeNameFloat:  arrow.PrimitiveTypes.Float32, // 4-byte single-precision
	sql.ColumnInfoTypeNameDouble: arrow.PrimitiveTypes.Float64, // 8-byte double-precision

	// Date-time types
	sql.ColumnInfoTypeNameDate: arrow.FixedWidthTypes.Date32, // year, month, day without timezone

	// Simple types
	sql.ColumnInfoTypeNameString:  arrow.BinaryTypes.String,      // character string values
	sql.ColumnInfoTypeNameBinary:  arrow.BinaryTypes.Binary,      // byte sequence values
	sql.ColumnInfoTypeNameBoolean: arrow.FixedWidthTypes.Boolean, // Boolean values
	sql.ColumnInfoTypeNameNull:    arrow.Null,                    // untyped NULL - not supported by Delta Lake
}

var stringTypeToArrowTypeMap = map[string]arrow.DataType{
	// Integral numeric types (whole numbers)
	"TINYINT":  arrow.PrimitiveTypes.Int8,  // 1-byte signed integer
	"BYTE":     arrow.PrimitiveTypes.Int8,  // 1-byte signed integer
	"SMALLINT": arrow.PrimitiveTypes.Int16, // 2-byte signed integer
	"SHORT":    arrow.PrimitiveTypes.Int16, // 2-byte signed integer
	"INT":      arrow.PrimitiveTypes.Int32, // 4-byte signed integer
	"INTEGER":  arrow.PrimitiveTypes.Int32, // 4-byte signed integer
	"LONG":     arrow.PrimitiveTypes.Int64, // 8-byte signed integer
	"BIGINT":   arrow.PrimitiveTypes.Int64, // 8-byte signed integer

	// Binary floating point types
	"FLOAT":  arrow.PrimitiveTypes.Float32, // 4-byte single-precision
	"REAL":   arrow.PrimitiveTypes.Float32, // 4-byte single-precision
	"DOUBLE": arrow.PrimitiveTypes.Float64, // 8-byte double-precision

	// Date-time types
	"DATE": arrow.FixedWidthTypes.Date32, // year, month, day without timezone

	// Simple types
	"STRING":  arrow.BinaryTypes.String,      // character string values
	"BINARY":  arrow.BinaryTypes.Binary,      // byte sequence values
	"BOOLEAN": arrow.FixedWidthTypes.Boolean, // Boolean values
	"VOID":    arrow.Null,                    // untyped NULL - not supported by Delta Lake
	"NULL":    arrow.Null,                    // untyped NULL - not supported by Delta Lake
}

// tracks bracket and parenthesis depth for parsing nested types
type depthTracker struct {
	bracketDepth int
	parenDepth   int
}

// updates the depth counters based on the given character
func (dt *depthTracker) updateDepth(char byte) {
	switch char {
	case '<':
		dt.bracketDepth++
	case '>':
		dt.bracketDepth--
	case '(':
		dt.parenDepth++
	case ')':
		dt.parenDepth--
	}
}

// returns true if both bracket and parenthesis depths are 0
func (dt *depthTracker) isAtTopLevel() bool {
	return dt.bracketDepth == 0 && dt.parenDepth == 0
}

func ClusterModeSchemaToArrowSchema(dbxSchema []map[string]interface{}) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(dbxSchema))
	for i, col := range dbxSchema {
		var arrowType arrow.DataType
		colType := strings.Trim(col["type"].(string), "\"")
		switch {
		case colType == "int":
			arrowType = arrow.PrimitiveTypes.Int32
		case colType == "integer":
			arrowType = arrow.PrimitiveTypes.Int32
		case colType == "string":
			arrowType = arrow.BinaryTypes.String
		case colType == "short":
			arrowType = arrow.PrimitiveTypes.Int16
		case colType == "long":
			arrowType = arrow.PrimitiveTypes.Int64
		case colType == "float":
			arrowType = arrow.PrimitiveTypes.Float32
		case colType == "double":
			arrowType = arrow.PrimitiveTypes.Float64
		case colType == "boolean":
			arrowType = arrow.FixedWidthTypes.Boolean
		case colType == "timestamp":
			arrowType = &arrow.TimestampType{Unit: arrow.Second}
		case colType == "date":
			arrowType = arrow.FixedWidthTypes.Date32
		case strings.HasPrefix(colType, "decimal"):
			// Parse decimal precision and scale from format "decimal(precision,scale)"
			if matches := regexp.MustCompile(`decimal\((\d+),(\d+)\)`).FindStringSubmatch(colType); matches != nil {
				precision, _ := strconv.ParseInt(matches[1], 10, 8)
				scale, _ := strconv.ParseInt(matches[2], 10, 8)
				arrowType = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
			} else {
				arrowType = &arrow.Decimal128Type{}
			}
		default:
			err := fmt.Errorf("unsupported type: %v", colType)
			return nil, err
		}
		fields[i] = arrow.Field{
			Name:     col["name"].(string),
			Type:     arrowType,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				DbxSchemaTypeText: colType,
			}),
		}
	}
	// TODO: include relevant metadata from dbrx into the Arrow schema
	return arrow.NewSchema(fields, nil), nil
}

func ResultSchemaToArrowSchema(dbxSchema *sql.ResultSchema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, dbxSchema.ColumnCount)
	for i, col := range dbxSchema.Columns {
		arrowType, err := getArrowTypeFromColumnInfo(col)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"type_name":       string(col.TypeName),
				"position":        col.Name,
				DbxSchemaTypeText: col.TypeText,
			}),
		}
	}
	metadata := arrow.MetadataFrom(map[string]string{
		"column_count": strconv.Itoa(dbxSchema.ColumnCount),
	})
	return arrow.NewSchema(fields, &metadata), nil
}

// Basic recursive descent parsing
func getArrowTypeFromStringType(col string) (arrow.DataType, error) {
	// handle simple, non-recursive types
	if arrowType, ok := stringTypeToArrowTypeMap[col]; ok {
		return arrowType, nil
	}
	if col == "TIMESTAMP" || col == "TIMESTAMP_NTZ" {
		return &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "Etc/UTC",
		}, nil
	}
	// { DECIMAL | DEC | NUMERIC } [ (  p [ , s ] ) ]
	if matches := regexp.MustCompile(decimalTypeRegex).FindStringSubmatch(col); matches != nil {
		var err error
		var precision64 int64
		precision64, err = strconv.ParseInt(matches[1], 10, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid decimal precision: %v", err)
		}
		precision := int(precision64)

		var scale = 0
		if len(matches) > 2 && matches[2] != "" {
			var scale64 int64
			scale64, err = strconv.ParseInt(matches[2], 10, 8)
			if err != nil {
				return nil, fmt.Errorf("invalid decimal scale: %v", err)
			}
			scale = int(scale64)
		}

		return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
	}
	// ARRAY < elementType >
	if matches := regexp.MustCompile(arrayTypeRegex).FindStringSubmatch(col); matches != nil {
		elementType := strings.TrimSpace(matches[1])
		elementArrowType, err := getArrowTypeFromStringType(elementType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse array element type: %w", err)
		}
		return arrow.ListOf(elementArrowType), nil
	}
	// MAP <keyType, valueType>
	if matches := regexp.MustCompile(mapTypeRegex).FindStringSubmatch(col); matches != nil {
		// Parse key and value types, handling nested commas
		keyType, valueType, err := parseMapKeyValue(matches[1])
		if err != nil {
			return nil, fmt.Errorf("invalid map type format: %w", err)
		}

		keyArrowType, err := getArrowTypeFromStringType(keyType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse map key type: %w", err)
		}
		valueArrowType, err := getArrowTypeFromStringType(valueType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse map value type: %w", err)
		}
		return arrow.MapOf(keyArrowType, valueArrowType), nil
	}
	// INTERVAL types - comprehensive pattern for all interval types
	if matches := regexp.MustCompile(intervalTypeRegex).FindStringSubmatch(col); matches != nil {
		// Year-month intervals
		if strings.Contains(matches[1], "YEAR") || matches[1] == "MONTH" {
			return arrow.FixedWidthTypes.MonthInterval, nil
		}
		// All other intervals are day-time intervals
		return arrow.FixedWidthTypes.Duration_s, nil
	}
	// STRUCT: STRUCT < [fieldName [:] fieldType [NOT NULL] [COLLATE collationName] [COMMENT str] [, …] ] >
	if matches := regexp.MustCompile(structTypeRegex).FindStringSubmatch(col); matches != nil {
		inner := matches[1]
		fields := parseStructFields(inner)
		arrowFields := make([]arrow.Field, 0, len(fields))
		// Parse field name and type (fieldName: fieldType)
		for _, field := range fields {
			fieldName, fieldType, nullable, err := parseStructField(field)
			if err != nil {
				return nil, fmt.Errorf("failed to parse struct field '%s': %w", field, err)
			}

			// Recursively parse the field type
			fieldArrowType, err := getArrowTypeFromStringType(fieldType)
			if err != nil {
				return nil, fmt.Errorf("failed to parse struct field type %s: %w", fieldName, err)
			}

			arrowFields = append(arrowFields, arrow.Field{
				Name:     fieldName,
				Type:     fieldArrowType,
				Nullable: nullable,
			})
		}
		return arrow.StructOf(arrowFields...), nil
	}
	// default to an unrecognized type so we can still positionally propogate metadata
	return arrow.Null, fmt.Errorf("unrecognized type: %s", col)
}

// parses comma separated struct fields and handles nesting
func parseStructFields(fieldsStr string) []string {
	var fields []string
	var currentField strings.Builder
	dt := &depthTracker{}

	for i := 0; i < len(fieldsStr); i++ {
		char := fieldsStr[i]

		switch char {
		case '<', '>', '(', ')':
			dt.updateDepth(char)
			currentField.WriteByte(char)
		case ',':
			if dt.isAtTopLevel() {
				// This comma separates fields, not nested type parameters
				field := strings.TrimSpace(currentField.String())
				if field != "" {
					fields = append(fields, field)
				}
				currentField.Reset()
				continue
			} else {
				// This comma is inside brackets or parentheses, so it's part of the type
				currentField.WriteByte(char)
			}
		default:
			currentField.WriteByte(char)
		}
	}
	// last field
	field := strings.TrimSpace(currentField.String())
	if field != "" {
		fields = append(fields, field)
	}
	return fields
}

// parses a single struct field
func parseStructField(field string) (fieldName, fieldType string, nullable bool, err error) {
	// Split on first colon to get field name and the rest
	parts := strings.SplitN(field, ":", 2)
	if len(parts) != 2 {
		return "", "", true, fmt.Errorf("invalid struct field format: missing colon")
	}

	fieldName = strings.TrimSpace(parts[0])
	rest := strings.TrimSpace(parts[1])

	// Extract field type and modifiers
	fieldType, nullable = extractFieldTypeAndModifiers(rest)

	return fieldName, fieldType, nullable, nil
}

// extracts the field type and determines nullability
// todo(jason): handle comments and collations (if they show up from the API)
func extractFieldTypeAndModifiers(rest string) (fieldType string, nullable bool) {
	// Default to nullable
	nullable = true

	// Split by whitespace to find modifiers
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return "", true
	}

	// The first part is the base type, but it might contain nested parentheses
	// We need to find where the type ends and modifiers begin
	typeEnd := findTypeEnd(rest)
	fieldType = strings.TrimSpace(rest[:typeEnd])

	// Check for NOT NULL modifier
	remaining := strings.TrimSpace(rest[typeEnd:])
	if strings.Contains(strings.ToUpper(remaining), "NOT NULL") {
		nullable = false
	}

	return fieldType, nullable
}

// finds the end of the type definition, handling nested brackets and multi-word types
func findTypeEnd(s string) int {
	dt := &depthTracker{}
	if strings.HasPrefix(s, "INTERVAL") {
		// Match INTERVAL types at the start
		intervalRegex := regexp.MustCompile(intervalTypePrefixRegex)
		if m := intervalRegex.FindStringIndex(s); m != nil {
			return m[1]
		}
	}
	for i := 0; i < len(s); i++ {
		char := s[i]
		switch char {
		case '<', '>':
			dt.updateDepth(char)
		case ' ':
			if dt.isAtTopLevel() {
				return i
			}
		}
	}
	return len(s)
}

// parseMapKeyValue parses the key and value types from a MAP type string
func parseMapKeyValue(mapContent string) (keyType, valueType string, err error) {
	var currentType strings.Builder
	dt := &depthTracker{}

	for i := 0; i < len(mapContent); i++ {
		char := mapContent[i]

		switch char {
		case '<', '>', '(', ')':
			dt.updateDepth(char)
			currentType.WriteByte(char)
		case ',':
			if dt.isAtTopLevel() {
				// This comma separates key and value types
				keyType = strings.TrimSpace(currentType.String())
				valueType = strings.TrimSpace(mapContent[i+1:])
				return keyType, valueType, nil
			} else {
				// This comma is inside nested brackets or parentheses, so it's part of the type
				currentType.WriteByte(char)
			}
		default:
			currentType.WriteByte(char)
		}
	}

	return "", "", fmt.Errorf("no comma found to separate key and value types")
}

func getArrowTypeFromColumnInfo(col sql.ColumnInfo) (arrow.DataType, error) {
	if arrowType, ok := basicTypeToArrowTypeMap[col.TypeName]; ok {
		return arrowType, nil
	}
	switch col.TypeName {
	case sql.ColumnInfoTypeNameDecimal:
		precision, scale := col.TypePrecision, col.TypeScale
		if precision == 0 {
			return getArrowTypeFromStringType(col.TypeText)
		}
		if precision <= 38 {
			return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
		}
		return &arrow.Decimal256Type{Precision: int32(precision), Scale: int32(scale)}, nil
	// todo(jasonlin45): Add support for TIMESTAMP_NTZ
	case sql.ColumnInfoTypeNameTimestamp:
		return &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "Etc/UTC", // todo(jasonlin45): Support session timezone
		}, nil
	case sql.ColumnInfoTypeNameArray,
		sql.ColumnInfoTypeNameMap,
		sql.ColumnInfoTypeNameStruct,
		sql.ColumnInfoTypeNameInterval:
		return getArrowTypeFromStringType(col.TypeText)
	default:
		return getArrowTypeFromStringType(col.TypeText)
	}
}
