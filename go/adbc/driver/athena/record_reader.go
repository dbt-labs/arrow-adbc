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

package athena

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// newRecordReader builds an array.RecordReader from Athena rows + column metadata.
// colInfo comes from ResultSetMetadata.ColumnInfo; rows must have the header already skipped.
func newRecordReader(alloc memory.Allocator, colInfo []types.ColumnInfo, rows []types.Row) (array.RecordReader, error) {
	schema := buildSchema(colInfo)

	if len(rows) == 0 {
		return array.NewRecordReader(schema, nil)
	}

	batch, err := buildRecordBatch(alloc, schema, colInfo, rows)
	if err != nil {
		return nil, err
	}
	defer batch.Release()

	return array.NewRecordReader(schema, []arrow.RecordBatch{batch})
}

// buildSchema converts Athena ColumnInfo slice to an Arrow schema.
func buildSchema(colInfo []types.ColumnInfo) *arrow.Schema {
	fields := make([]arrow.Field, len(colInfo))
	for i, col := range colInfo {
		name := ""
		if col.Name != nil {
			name = *col.Name
		}
		fields[i] = arrow.Field{
			Name:     name,
			Type:     athenaColumnTypeToArrow(col),
			Nullable: true,
		}
	}
	return arrow.NewSchema(fields, nil)
}

// athenaColumnTypeToArrow maps a ColumnInfo's type to an Arrow DataType.
func athenaColumnTypeToArrow(col types.ColumnInfo) arrow.DataType {
	if col.Type == nil {
		return arrow.BinaryTypes.String
	}
	return athenaTypeStringToArrow(*col.Type)
}

// athenaTypeStringToArrow maps an Athena type string to an Arrow DataType.
func athenaTypeStringToArrow(t string) arrow.DataType {
	switch t {
	case "varchar", "string", "char":
		return arrow.BinaryTypes.String
	case "bigint":
		return arrow.PrimitiveTypes.Int64
	case "integer", "int":
		return arrow.PrimitiveTypes.Int32
	case "smallint":
		return arrow.PrimitiveTypes.Int16
	case "tinyint":
		return arrow.PrimitiveTypes.Int8
	case "double":
		return arrow.PrimitiveTypes.Float64
	case "float", "real":
		return arrow.PrimitiveTypes.Float32
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "date":
		return arrow.FixedWidthTypes.Date32
	case "timestamp", "timestamp with time zone":
		return arrow.FixedWidthTypes.Timestamp_us
	case "varbinary", "binary":
		return arrow.BinaryTypes.Binary
	default:
		// array, map, row, decimal, json — stringify
		return arrow.BinaryTypes.String
	}
}

// buildRecordBatch converts Athena rows into a single Arrow RecordBatch.
func buildRecordBatch(alloc memory.Allocator, schema *arrow.Schema, colInfo []types.ColumnInfo, rows []types.Row) (arrow.RecordBatch, error) {
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	for _, row := range rows {
		for ci, field := range row.Data {
			if ci >= len(colInfo) {
				break
			}
			val := ""
			isNull := field.VarCharValue == nil
			if !isNull {
				val = *field.VarCharValue
			}
			fb := bldr.Field(ci)
			if err := appendValue(fb, colInfo[ci], val, isNull); err != nil {
				return nil, fmt.Errorf("column %d (%s): %w", ci, schema.Field(ci).Name, err)
			}
		}
	}

	return bldr.NewRecord(), nil
}

// appendValue appends a string-encoded value to the appropriate builder type.
func appendValue(bldr array.Builder, col types.ColumnInfo, val string, isNull bool) error {
	if isNull {
		bldr.AppendNull()
		return nil
	}

	colType := ""
	if col.Type != nil {
		colType = *col.Type
	}

	switch colType {
	case "bigint":
		v, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
		bldr.(*array.Int64Builder).Append(v)
	case "integer", "int":
		v, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return err
		}
		bldr.(*array.Int32Builder).Append(int32(v))
	case "smallint":
		v, err := strconv.ParseInt(val, 10, 16)
		if err != nil {
			return err
		}
		bldr.(*array.Int16Builder).Append(int16(v))
	case "tinyint":
		v, err := strconv.ParseInt(val, 10, 8)
		if err != nil {
			return err
		}
		bldr.(*array.Int8Builder).Append(int8(v))
	case "double":
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return err
		}
		bldr.(*array.Float64Builder).Append(v)
	case "float", "real":
		v, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return err
		}
		bldr.(*array.Float32Builder).Append(float32(v))
	case "boolean":
		v, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		bldr.(*array.BooleanBuilder).Append(v)
	case "date":
		days, err := parseDateToDays(val)
		if err != nil {
			return err
		}
		bldr.(*array.Date32Builder).Append(arrow.Date32(days))
	case "timestamp", "timestamp with time zone":
		us, err := parseTimestampToMicros(val)
		if err != nil {
			return err
		}
		bldr.(*array.TimestampBuilder).Append(arrow.Timestamp(us))
	case "varbinary", "binary":
		bldr.(*array.BinaryBuilder).Append([]byte(val))
	default:
		// varchar, string, char, array, map, row, decimal, json, etc.
		bldr.(*array.StringBuilder).Append(val)
	}
	return nil
}

// parseDateToDays parses "YYYY-MM-DD" and returns days since Unix epoch (1970-01-01).
// Uses the civil date algorithm from http://howardhinnant.github.io/date_algorithms.html
func parseDateToDays(s string) (int32, error) {
	if len(s) < 10 {
		return 0, fmt.Errorf("unexpected date format: %q", s)
	}
	year, err := strconv.Atoi(s[0:4])
	if err != nil {
		return 0, err
	}
	month, err := strconv.Atoi(s[5:7])
	if err != nil {
		return 0, err
	}
	day, err := strconv.Atoi(s[8:10])
	if err != nil {
		return 0, err
	}
	return civilToDays(year, month, day), nil
}

// civilToDays converts a Gregorian calendar date to days since Unix epoch.
// Algorithm: http://howardhinnant.github.io/date_algorithms.html days_from_civil
func civilToDays(y, m, d int) int32 {
	if m <= 2 {
		y--
		m += 9
	} else {
		m -= 3
	}
	era := y / 400
	if y < 0 {
		era = (y - 399) / 400
	}
	yoe := y - era*400
	doy := (153*m+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return int32(era*146097 + doe - 719468)
}

// parseTimestampToMicros parses an Athena timestamp string to microseconds since Unix epoch.
// Athena timestamps use the format "YYYY-MM-DD HH:MM:SS.ffffff" (fractional part optional).
func parseTimestampToMicros(s string) (int64, error) {
	if len(s) < 19 {
		return 0, fmt.Errorf("unexpected timestamp format: %q", s)
	}
	year, err := strconv.Atoi(s[0:4])
	if err != nil {
		return 0, err
	}
	month, err := strconv.Atoi(s[5:7])
	if err != nil {
		return 0, err
	}
	day, err := strconv.Atoi(s[8:10])
	if err != nil {
		return 0, err
	}
	hour, err := strconv.Atoi(s[11:13])
	if err != nil {
		return 0, err
	}
	min, err := strconv.Atoi(s[14:16])
	if err != nil {
		return 0, err
	}
	sec, err := strconv.Atoi(s[17:19])
	if err != nil {
		return 0, err
	}

	var fracMicros int64
	if len(s) > 20 {
		frac := s[20:]
		// pad or truncate to 6 digits
		for len(frac) < 6 {
			frac += "0"
		}
		if len(frac) > 6 {
			frac = frac[:6]
		}
		fracMicros, err = strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, err
		}
	}

	days := civilToDays(year, month, day)
	totalMicros := int64(days)*86400*1_000_000 +
		int64(hour)*3600*1_000_000 +
		int64(min)*60*1_000_000 +
		int64(sec)*1_000_000 +
		fracMicros

	return totalMicros, nil
}
