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

package redshift

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

type RecordBatchReader struct {
	refCount     int64
	ctx          context.Context
	cnxn         *connectionImpl
	alloc        memory.Allocator
	schema       *arrow.Schema
	currentBatch arrow.Record
	statementId  string
	nextTokenId  string // empty when no next batch
	err          error
	initialized  bool // Tracks whether the first batch has been fetched
}

// NewRecordBatchReader initializes the RecordBatchReader but does not fetch the first batch.
func NewRecordBatchReader(
	ctx context.Context,
	cnxn *connectionImpl,
	alloc memory.Allocator,
	statementID string,
) (*RecordBatchReader, error) {
	return &RecordBatchReader{
		refCount:    1,
		ctx:         ctx,
		cnxn:        cnxn,
		alloc:       alloc,
		statementId: statementID,
		nextTokenId: "",
		initialized: false,
	}, nil
}

// Schema returns the schema of the records.
func (rr *RecordBatchReader) Schema() *arrow.Schema {
	return rr.schema
}

func (rr *RecordBatchReader) Retain() {
	atomic.AddInt64(&rr.refCount, 1)
}

// Map type to Arrow types
func mapRedshiftColumnMetaToArrowField(col types.ColumnMetadata) (*arrow.Field, error) {
	out := &arrow.Field{
		Name:     *col.Name,
		Nullable: col.Nullable != 0,
	}
	switch strings.ToLower(*col.TypeName) {
	case "int2": // SMALLINT
		out.Type = arrow.PrimitiveTypes.Int16
	case "int", "int4": // INTEGER
		out.Type = arrow.PrimitiveTypes.Int32
	case "int8": // BIGINT
		out.Type = arrow.PrimitiveTypes.Int64
	case "float4": // REAL
		out.Type = arrow.PrimitiveTypes.Float32
	case "float", "float8": // DOUBLE PRECISION
		out.Type = arrow.PrimitiveTypes.Float64
	case "bool": // BOOLEAN
		out.Type = arrow.FixedWidthTypes.Boolean
	case "char", "varchar", "text": // CHARACTER, VARCHAR, TEXT
		out.Type = arrow.BinaryTypes.String
	case "date": // DATE
		out.Type = arrow.FixedWidthTypes.Date32
	case "time", "timetz": // TIME, TIME WITH TIME ZONE
		out.Type = arrow.FixedWidthTypes.Time64us
	case "timestamp", "timestamptz": // TIMESTAMP, TIMESTAMP WITH TIME ZONE
		out.Type = arrow.FixedWidthTypes.Timestamp_us
	case "decimal", "numeric": // DECIMAL/NUMERIC
		out.Type = &arrow.Decimal128Type{Precision: col.Precision, Scale: col.Scale}
	default:
		// TODO: Handle other types or return an error
		return nil, fmt.Errorf("unsupported column type: %s", *col.TypeName)
	}
	return out, nil
}

func (rr *RecordBatchReader) Initialize() {
	resultsOut, err := rr.cnxn.client.GetStatementResult(rr.ctx, &redshiftdata.GetStatementResultInput{
		Id: aws.String(rr.statementId),
	})

	if err != nil {
		rr.err = err
		return
	}

	// Deserialize schema from Redshift metadata
	fields := make([]arrow.Field, len(resultsOut.ColumnMetadata))

	for i, col := range resultsOut.ColumnMetadata {
		arrowField, err := mapRedshiftColumnMetaToArrowField(col)
		if err != nil {
			rr.err = err
			return
		}
		fields[i] = *arrowField
	}
	// Create the Arrow schema
	rr.schema = arrow.NewSchema(fields, nil)
	rr.currentBatch, rr.err = rr.convertRecords(rr.schema, resultsOut.Records)
	if rr.err != nil {
		panic(rr.err)
	}

	// Set the next token for subsequent batches
	rr.nextTokenId = aws.ToString(resultsOut.NextToken)
}

// Next fetches the next batch of records.
// The first call to Next initializes the schema and retrieves the first batch.
func (rr *RecordBatchReader) Next() bool {
	if rr.err != nil {
		return false
	}

	// S0: Complete Initialization
	if !rr.initialized {
		rr.initialized = true
		return rr.currentBatch != nil
	}

	// S1: Next batch
	if rr.nextTokenId != "" {
		resultsOut, err := rr.cnxn.client.GetStatementResult(rr.ctx, &redshiftdata.GetStatementResultInput{
			Id:        aws.String(rr.statementId),
			NextToken: aws.String(rr.nextTokenId),
		})

		if err != nil {
			rr.err = err
			return false
		}
		rr.currentBatch.Release()
		rr.currentBatch, rr.err = rr.convertRecords(rr.schema, resultsOut.Records)
		if rr.err != nil {
			return false
		}

		// Update the next token
		rr.nextTokenId = aws.ToString(resultsOut.NextToken)
		return true
	}
	return false
}

// Returns the current record batch.
func (rr *RecordBatchReader) Record() arrow.Record {
	if rr.err != nil {
		return nil
	}
	return rr.currentBatch
}

// Err returns any error encountered during reading.
func (rr *RecordBatchReader) Err() error {
	return rr.err
}

// Release releases resources associated with the RecordBatchReader.
func (rr *RecordBatchReader) Release() {
	if atomic.AddInt64(&rr.refCount, -1) == 0 {
		rr.currentBatch.Release()
		rr.currentBatch = nil
		rr.schema = nil
	}
}

// convertRecords converts Redshift records to Arrow records.
func (rr *RecordBatchReader) convertRecords(schema *arrow.Schema, records [][]types.Field) (arrow.Record, error) {
	// Implement conversion logic here
	// This will involve creating Arrow arrays for each column and combining them into Arrow records
	capacity := len(records)
	builder, err := NewAdbcRecordBatchBuilder(schema, capacity, rr.alloc)
	defer builder.Release()
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		builder.Append(record)
	}
	columns, err := builder.Finish()

	if err != nil {
		return nil, err
	}

	// Create Arrow record from columns
	record := array.NewRecord(rr.schema, columns, int64(capacity))

	for _, col := range columns {
		col.Release()
	}

	return record, nil
}

// extracts the raw value from a Redshift Field.
func getRawFieldValue(field types.Field) (any, error) {
	switch v := field.(type) {
	case *types.FieldMemberBlobValue:
		return v.Value, nil
	case *types.FieldMemberBooleanValue:
		return v.Value, nil
	case *types.FieldMemberDoubleValue:
		return v.Value, nil
	case *types.FieldMemberLongValue:
		return v.Value, nil
	case *types.FieldMemberStringValue:
		return v.Value, nil
	case *types.FieldMemberIsNull:
		if v.Value {
			return nil, nil // Field is explicitly NULL
		}
		return nil, fmt.Errorf("unexpected null field value")
	default:
		return nil, fmt.Errorf("unsupported field type: %T", field)
	}
}

type AdbcRecordBatchBuilder struct {
	schema          *arrow.Schema
	column_builders []ColumnBuilder
}

func NewAdbcRecordBatchBuilder(schema *arrow.Schema, capacity int, alloc memory.Allocator) (*AdbcRecordBatchBuilder, error) {
	fields := schema.Fields()
	builders := make([]ColumnBuilder, len(fields))
	for i, field := range fields {
		builder, err := NewColumnBuilder(field, capacity, alloc)
		if err != nil {
			return nil, err
		}
		builders[i] = builder
	}
	return &AdbcRecordBatchBuilder{
		schema:          schema,
		column_builders: builders,
	}, nil
}

func (adbc *AdbcRecordBatchBuilder) Append(record []types.Field) error {
	for i, field := range record {
		if err := adbc.column_builders[i].Append(field); err != nil {
			return err
		}
	}
	return nil
}

func (adbc *AdbcRecordBatchBuilder) Finish() ([]arrow.Array, error) {
	// Create Arrow arrays for each column
	arrays := make([]arrow.Array, len(adbc.column_builders))
	for i, builder := range adbc.column_builders {
		arr, err := builder.Finish()
		if err != nil {
			for _, a := range arrays {
				if a != nil {
					a.Release()
				}
			}
			return nil, err
		}
		arrays[i] = *arr
	}
	return arrays, nil
}

func (adbc *AdbcRecordBatchBuilder) Release() {
	for _, builder := range adbc.column_builders {
		if builder != nil {
			builder.Finish()
		}
	}
	adbc.column_builders = nil
	adbc.schema = nil
}

// Column Builder
type ColumnBuilder interface {
	Append(value types.Field) error
	Finish() (*arrow.Array, error)
}

func NewColumnBuilder(field arrow.Field, capacity int, alloc memory.Allocator) (ColumnBuilder, error) {
	switch field.Type.ID() {
	case arrow.INT16:
		return NewInt16ColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.INT32:
		return NewInt32ColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.INT64:
		return NewInt64ColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.FLOAT32:
		return NewFloat32ColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.FLOAT64:
		return NewFloat64ColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.DECIMAL128:
		return NewDecimal128ColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.BOOL:
		return NewBooleanColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.DATE32:
		return NewDateColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.TIME64:
		return NewTimeColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.TIMESTAMP:
		return NewTimestampColumnBuilder(field.Type, capacity, alloc), nil
	case arrow.STRING:
		return NewStringColumnBuilder(field.Type, capacity, alloc), nil
	}
	return nil, fmt.Errorf("unsupported column type: %s", field.Type)
}

// BOOLEAN
type BooleanColumnBuilder struct {
	builder *array.BooleanBuilder
}

func NewBooleanColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := BooleanColumnBuilder{
		builder: array.NewBooleanBuilder(alloc),
	}
	return ColumnBuilder(&builder)
}

func (b *BooleanColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.Append(rawValue.(bool))
	}
	return nil
}

func (b *BooleanColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	b.builder.Release()
	return &arr, nil
}

// INT16
type Int16ColumnBuilder struct {
	builder *array.Int16Builder
}

func NewInt16ColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := Int16ColumnBuilder{
		builder: array.NewInt16Builder(alloc),
	}
	return ColumnBuilder(&builder)
}

func (b *Int16ColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	castValue := int16(rawValue.(int64))
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.Append(castValue)
	}
	return nil
}

func (b *Int16ColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}

// INT32
type Int32ColumnBuilder struct {
	builder *array.Int32Builder
}

func NewInt32ColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := Int32ColumnBuilder{
		builder: array.NewInt32Builder(alloc),
	}
	return ColumnBuilder(&builder)
}

func (b *Int32ColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	castValue := int32(rawValue.(int64))
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.Append(castValue)
	}
	return nil
}

func (b *Int32ColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}

// INT64
type Int64ColumnBuilder struct {
	builder *array.Int64Builder
}

func NewInt64ColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := Int64ColumnBuilder{
		builder: array.NewInt64Builder(alloc),
	}
	return ColumnBuilder(&builder)
}

func (b *Int64ColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.Append(rawValue.(int64))
	}
	return nil
}

func (b *Int64ColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}

// DECIMAL
type Decimal128ColumnBuilder struct {
	builder   *array.Decimal128Builder
	precision int32
	scale     int32
}

func NewDecimal128ColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	castDt := dt.(*arrow.Decimal128Type)
	arrow.NewDecimalType(dt.ID(), castDt.Precision, castDt.Scale)
	builder := Decimal128ColumnBuilder{
		builder:   array.NewDecimal128Builder(alloc, dt.(*arrow.Decimal128Type)),
		precision: castDt.Precision,
		scale:     castDt.Scale,
	}
	return ColumnBuilder(&builder)
}

func (b *Decimal128ColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.AppendValueFromString(rawValue.(string))
	}
	return nil
}

func (b *Decimal128ColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}

// FLOAT32
type Float32ColumnBuilder struct {
	builder *array.Float32Builder
}

func NewFloat32ColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := Float32ColumnBuilder{
		builder: array.NewFloat32Builder(alloc),
	}
	return ColumnBuilder(&builder)
}

func (b *Float32ColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	castValue := float32(rawValue.(float64))
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.Append(castValue)
	}
	return nil
}

func (b *Float32ColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}

// FLOAT64
type Float64ColumnBuilder struct {
	builder *array.Float64Builder
}

func NewFloat64ColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := Float64ColumnBuilder{
		builder: array.NewFloat64Builder(alloc),
	}
	return ColumnBuilder(&builder)
}

func (b *Float64ColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.Append(rawValue.(float64))
	}
	return nil
}

func (b *Float64ColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}

// STRING
type StringColumnBuilder struct {
	builder *array.StringBuilder
}

func NewStringColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := StringColumnBuilder{
		builder: array.NewStringBuilder(alloc),
	}
	return ColumnBuilder(&builder)
}

func (b *StringColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.AppendValueFromString(rawValue.(string))
	}
	return nil
}

func (b *StringColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}

// TIME
type DateColumnBuilder struct {
	builder *array.Date32Builder
}

func NewDateColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := DateColumnBuilder{
		builder: array.NewDate32Builder(alloc),
	}
	return ColumnBuilder(&builder)
}

func (b *DateColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.AppendValueFromString(rawValue.(string))
	}
	return nil
}

func (b *DateColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}

// TIME
type TimeColumnBuilder struct {
	builder *array.Time64Builder
}

func NewTimeColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := TimeColumnBuilder{
		builder: array.NewTime64Builder(alloc, &arrow.Time64Type{Unit: arrow.Microsecond}),
	}
	return ColumnBuilder(&builder)
}

func (b *TimeColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.AppendValueFromString(rawValue.(string))
	}
	return nil
}

func (b *TimeColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}

// TIMESTAMP
type TimestampColumnBuilder struct {
	builder *array.TimestampBuilder
}

func NewTimestampColumnBuilder(dt arrow.DataType, capacity int, alloc memory.Allocator) ColumnBuilder {
	builder := TimestampColumnBuilder{
		builder: array.NewTimestampBuilder(alloc, dt.(*arrow.TimestampType)),
	}
	return ColumnBuilder(&builder)
}

func (b *TimestampColumnBuilder) Append(value types.Field) error {
	rawValue, err := getRawFieldValue(value)
	if err != nil {
		return err
	}
	if rawValue == nil {
		b.builder.AppendNull()
	} else {
		b.builder.AppendValueFromString(rawValue.(string))
	}
	return nil
}

func (b *TimestampColumnBuilder) Finish() (*arrow.Array, error) {
	arr := b.builder.NewArray()
	return &arr, nil
}
