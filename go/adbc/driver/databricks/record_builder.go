package databricks

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ColumnBuilder is an interface for building Arrow arrays
type ColumnBuilder interface {
	Append(value interface{}) error
	Builder() array.Builder
}

// StringColumnBuilder handles string values
type StringColumnBuilder struct {
	builder *array.StringBuilder
}

func NewStringColumnBuilder(mem memory.Allocator) *StringColumnBuilder {
	return &StringColumnBuilder{
		builder: array.NewStringBuilder(mem),
	}
}

func (b *StringColumnBuilder) Append(value interface{}) error {
	if value == nil {
		b.builder.AppendNull()
		return nil
	}
	if str, ok := value.(string); ok {
		b.builder.Append(str)
	} else {
		b.builder.Append(fmt.Sprintf("%v", value))
	}
	return nil
}

func (b *StringColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Int32ColumnBuilder handles int32 values
type Int32ColumnBuilder struct {
	builder *array.Int32Builder
}

func NewInt32ColumnBuilder(mem memory.Allocator) *Int32ColumnBuilder {
	return &Int32ColumnBuilder{
		builder: array.NewInt32Builder(mem),
	}
}

func (b *Int32ColumnBuilder) Append(value interface{}) error {
	if value == nil {
		b.builder.AppendNull()
		return nil
	}
	switch v := value.(type) {
	case int:
		b.builder.Append(int32(v))
	case int32:
		b.builder.Append(v)
	default:
		return fmt.Errorf("invalid type for Int32 column: %T", value)
	}
	return nil
}

func (b *Int32ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Int64ColumnBuilder handles int64 values
type Int64ColumnBuilder struct {
	builder *array.Int64Builder
}

func NewInt64ColumnBuilder(mem memory.Allocator) *Int64ColumnBuilder {
	return &Int64ColumnBuilder{
		builder: array.NewInt64Builder(mem),
	}
}

func (b *Int64ColumnBuilder) Append(value interface{}) error {
	if value == nil {
		b.builder.AppendNull()
		return nil
	}
	switch v := value.(type) {
	case int64:
		b.builder.Append(v)
	case int:
		b.builder.Append(int64(v))
	default:
		return fmt.Errorf("invalid type for Int64 column: %T", value)
	}
	return nil
}

func (b *Int64ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Float32ColumnBuilder handles float32 values
type Float32ColumnBuilder struct {
	builder *array.Float32Builder
}

func NewFloat32ColumnBuilder(mem memory.Allocator) *Float32ColumnBuilder {
	return &Float32ColumnBuilder{
		builder: array.NewFloat32Builder(mem),
	}
}

func (b *Float32ColumnBuilder) Append(value interface{}) error {
	if value == nil {
		b.builder.AppendNull()
		return nil
	}
	switch v := value.(type) {
	case float32:
		b.builder.Append(v)
	case float64:
		b.builder.Append(float32(v))
	default:
		return fmt.Errorf("invalid type for Float32 column: %T", value)
	}
	return nil
}

func (b *Float32ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Float64ColumnBuilder handles float64 values
type Float64ColumnBuilder struct {
	builder *array.Float64Builder
}

func NewFloat64ColumnBuilder(mem memory.Allocator) *Float64ColumnBuilder {
	return &Float64ColumnBuilder{
		builder: array.NewFloat64Builder(mem),
	}
}

func (b *Float64ColumnBuilder) Append(value interface{}) error {
	if value == nil {
		b.builder.AppendNull()
		return nil
	}
	switch v := value.(type) {
	case float64:
		b.builder.Append(v)
	case float32:
		b.builder.Append(float64(v))
	default:
		return fmt.Errorf("invalid type for Float64 column: %T", value)
	}
	return nil
}

func (b *Float64ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// BooleanColumnBuilder handles boolean values
type BooleanColumnBuilder struct {
	builder *array.BooleanBuilder
}

func NewBooleanColumnBuilder(mem memory.Allocator) *BooleanColumnBuilder {
	return &BooleanColumnBuilder{
		builder: array.NewBooleanBuilder(mem),
	}
}

func (b *BooleanColumnBuilder) Append(value interface{}) error {
	if value == nil {
		b.builder.AppendNull()
		return nil
	}
	if v, ok := value.(bool); ok {
		b.builder.Append(v)
	} else {
		return fmt.Errorf("invalid type for Boolean column: %T", value)
	}
	return nil
}

func (b *BooleanColumnBuilder) Builder() array.Builder {
	return b.builder
}

// TimestampColumnBuilder handles timestamp values
type TimestampColumnBuilder struct {
	builder *array.TimestampBuilder
}

func NewTimestampColumnBuilder(mem memory.Allocator) *TimestampColumnBuilder {
	return &TimestampColumnBuilder{
		builder: array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond}),
	}
}

func (b *TimestampColumnBuilder) Append(value interface{}) error {
	if value == nil {
		b.builder.AppendNull()
		return nil
	}
	if t, ok := value.(time.Time); ok {
		b.builder.Append(arrow.Timestamp(t.UnixMicro()))
	} else {
		return fmt.Errorf("invalid type for Timestamp column: %T", value)
	}
	return nil
}

func (b *TimestampColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Date32ColumnBuilder handles date32 values
type Date32ColumnBuilder struct {
	builder *array.Date32Builder
}

func NewDate32ColumnBuilder(mem memory.Allocator) *Date32ColumnBuilder {
	return &Date32ColumnBuilder{
		builder: array.NewDate32Builder(mem),
	}
}

func (b *Date32ColumnBuilder) Append(value interface{}) error {
	if value == nil {
		b.builder.AppendNull()
		return nil
	}
	if t, ok := value.(time.Time); ok {
		b.builder.Append(arrow.Date32(t.Unix() / 86400))
	} else {
		return fmt.Errorf("invalid type for Date32 column: %T", value)
	}
	return nil
}

func (b *Date32ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// RecordBuilder handles building Arrow records from arrays of data
type RecordBuilder struct {
	schema   *arrow.Schema
	mem      memory.Allocator
	builders []ColumnBuilder
}

// NewRecordBuilder creates a new RecordBuilder with the given schema
func NewRecordBuilder(schema *arrow.Schema) *RecordBuilder {
	mem := memory.NewGoAllocator()
	builders := make([]ColumnBuilder, len(schema.Fields()))

	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.STRING:
			builders[i] = NewStringColumnBuilder(mem)
		case arrow.INT32:
			builders[i] = NewInt32ColumnBuilder(mem)
		case arrow.INT64:
			builders[i] = NewInt64ColumnBuilder(mem)
		case arrow.FLOAT32:
			builders[i] = NewFloat32ColumnBuilder(mem)
		case arrow.FLOAT64:
			builders[i] = NewFloat64ColumnBuilder(mem)
		case arrow.BOOL:
			builders[i] = NewBooleanColumnBuilder(mem)
		case arrow.TIMESTAMP:
			builders[i] = NewTimestampColumnBuilder(mem)
		case arrow.DATE32:
			builders[i] = NewDate32ColumnBuilder(mem)
		default:
			// Default to string for unknown types
			builders[i] = NewStringColumnBuilder(mem)
		}
	}

	return &RecordBuilder{
		schema:   schema,
		mem:      mem,
		builders: builders,
	}
}

// Append appends a single row of data to the builders
func (rb *RecordBuilder) Append(row []interface{}) error {
	if len(row) != len(rb.builders) {
		return fmt.Errorf("row length %d does not match schema length %d", len(row), len(rb.builders))
	}

	for i, val := range row {
		if err := rb.builders[i].Append(val); err != nil {
			return err
		}
	}
	return nil
}

// NewRecord creates a new Arrow record from the accumulated data
func (rb *RecordBuilder) NewRecord() arrow.Record {
	fields := make([]arrow.Array, len(rb.builders))
	for i, builder := range rb.builders {
		fields[i] = builder.Builder().NewArray()
	}
	return array.NewRecord(rb.schema, fields, int64(fields[0].Len()))
}

// Release releases the memory allocated by the builders
func (rb *RecordBuilder) Release() {
	for _, builder := range rb.builders {
		builder.Builder().Release()
	}
}

// BuildFromRows creates a new Arrow record from an array of rows
func BuildFromRows(schema *arrow.Schema, rows []interface{}) (arrow.Record, error) {
	rb := NewRecordBuilder(schema)
	defer rb.Release()
	for _, row := range rows {
		if err := rb.Append(row.([]interface{})); err != nil {
			return nil, err
		}
	}

	return rb.NewRecord(), nil
}
