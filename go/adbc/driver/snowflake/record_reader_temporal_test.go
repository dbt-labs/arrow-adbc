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

package snowflake

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The wire encodings below are the ones Snowflake sends in Arrow results:
// TIME and TIMESTAMP_NTZ/LTZ up to scale 7 are integer counts of 10^-scale
// seconds (since midnight or the epoch). TIMESTAMP_NTZ/LTZ of scale 8 and 9
// are structs of the epoch seconds and the nanoseconds (whatever the scale).
// TIMESTAMP_TZ is a struct of the epoch, as an integer count of 10^-scale
// seconds up to scale 3 or as epoch seconds and nanoseconds above, and of the
// time zone offset in minutes plus 1440.

// 2024-01-02 03:04:05.123456789 UTC
var testTime = time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC)

// 1969-12-31 23:59:59.123456789 UTC, before the epoch
var testTimeBeforeEpoch = time.Date(1969, 12, 31, 23, 59, 59, 123456789, time.UTC)

// truncate drops the fractional digits of t beyond scale.
func truncate(t time.Time, scale int64) time.Time {
	return t.Truncate(time.Duration(pow10[9-scale]))
}

// scaledInts encodes times as integer counts of 10^-scale seconds since the epoch.
func scaledInts(mem memory.Allocator, scale int64, times ...*time.Time) arrow.Array {
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, t := range times {
		if t == nil {
			b.AppendNull()
			continue
		}
		b.Append(truncate(*t, scale).UnixNano() / pow10[9-scale])
	}
	return b.NewArray()
}

// epochFractions encodes times as structs of epoch seconds and nanoseconds,
// followed by a time zone offset field when offsets is not nil.
func epochFractions(mem memory.Allocator, scale int64, offsets []int32, times ...*time.Time) arrow.Array {
	fields := []arrow.Field{
		{Name: "epoch", Type: arrow.PrimitiveTypes.Int64},
		{Name: "fraction", Type: arrow.PrimitiveTypes.Int32},
	}
	if offsets != nil {
		fields = append(fields, arrow.Field{Name: "timezone", Type: arrow.PrimitiveTypes.Int32})
	}
	b := array.NewStructBuilder(mem, arrow.StructOf(fields...))
	defer b.Release()
	for i, t := range times {
		if t == nil {
			b.AppendNull()
			continue
		}
		v := truncate(*t, scale)
		b.Append(true)
		b.FieldBuilder(0).(*array.Int64Builder).Append(v.Unix())
		b.FieldBuilder(1).(*array.Int32Builder).Append(int32(v.Nanosecond()))
		if offsets != nil {
			b.FieldBuilder(2).(*array.Int32Builder).Append(offsets[i] + 1440)
		}
	}
	return b.NewArray()
}

// scaledIntsWithOffsets encodes times as structs of an integer count of
// 10^-scale seconds since the epoch and a time zone offset.
func scaledIntsWithOffsets(mem memory.Allocator, scale int64, offsets []int32, times ...*time.Time) arrow.Array {
	b := array.NewStructBuilder(mem, arrow.StructOf(
		arrow.Field{Name: "epoch", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "timezone", Type: arrow.PrimitiveTypes.Int32},
	))
	defer b.Release()
	for i, t := range times {
		if t == nil {
			b.AppendNull()
			continue
		}
		b.Append(true)
		b.FieldBuilder(0).(*array.Int64Builder).Append(truncate(*t, scale).UnixNano() / pow10[9-scale])
		b.FieldBuilder(1).(*array.Int32Builder).Append(offsets[i] + 1440)
	}
	return b.NewArray()
}

func testContext(mem memory.Allocator) context.Context {
	return compute.WithAllocator(context.Background(), mem)
}

func timestamps(t *testing.T, a arrow.Array) []*time.Time {
	ts := a.(*array.Timestamp)
	unit := ts.DataType().(*arrow.TimestampType).Unit
	out := make([]*time.Time, ts.Len())
	for i := range out {
		if ts.IsValid(i) {
			v := ts.Value(i).ToTime(unit)
			out[i] = &v
		}
	}
	return out
}

func TestTimeOptionsUnits(t *testing.T) {
	defaults := timeOptions{}
	micros := timeOptions{maxTimestampPrecision: Microseconds}
	lossless := timeOptions{losslessTimeUnit: true}
	losslessMicros := timeOptions{losslessTimeUnit: true, maxTimestampPrecision: Microseconds}

	s, ms, us, ns := arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond
	for scale, want := range [][4]arrow.TimeUnit{
		// defaults, micros, lossless, losslessMicros
		{s, s, s, s},
		{s, s, ms, ms},
		{s, s, ms, ms},
		{ms, ms, ms, ms},
		{ms, ms, us, us},
		{ms, ms, us, us},
		{us, us, us, us},
		{us, us, ns, us},
		{us, us, ns, us},
		{ns, us, ns, us},
	} {
		got := [4]arrow.TimeUnit{
			defaults.timestampUnit(int64(scale)),
			micros.timestampUnit(int64(scale)),
			lossless.timestampUnit(int64(scale)),
			losslessMicros.timestampUnit(int64(scale)),
		}
		assert.Equal(t, want, got, "scale %d", scale)
	}

	// TIME is not capped by the maximum timestamp precision
	assert.Equal(t, arrow.FixedWidthTypes.Time64ns, losslessMicros.timeType(7))
	assert.Equal(t, arrow.FixedWidthTypes.Time32ms, lossless.timeType(1))
	assert.Equal(t, arrow.FixedWidthTypes.Time32s, defaults.timeType(2))
	assert.Equal(t, arrow.FixedWidthTypes.Time64us, defaults.timeType(8))
}

func TestConvertTimestamps(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := testContext(mem)

	for _, tc := range []struct {
		name  string
		scale int64
		opts  timeOptions
		// the fractional digits the output keeps
		digits int64
	}{
		{"scale 0", 0, timeOptions{}, 0},
		{"scale 1", 1, timeOptions{}, 0},
		{"scale 1 lossless", 1, timeOptions{losslessTimeUnit: true}, 1},
		{"scale 2 lossless", 2, timeOptions{losslessTimeUnit: true}, 2},
		{"scale 3", 3, timeOptions{}, 3},
		{"scale 4", 4, timeOptions{}, 3},
		{"scale 5 lossless", 5, timeOptions{losslessTimeUnit: true}, 5},
		{"scale 6", 6, timeOptions{}, 6},
		{"scale 7", 7, timeOptions{}, 6},
		{"scale 7 lossless", 7, timeOptions{losslessTimeUnit: true}, 7},
		{"scale 7 lossless micros", 7, timeOptions{losslessTimeUnit: true, maxTimestampPrecision: Microseconds}, 6},
		{"scale 8", 8, timeOptions{}, 6},
		{"scale 8 lossless", 8, timeOptions{losslessTimeUnit: true}, 8},
		{"scale 9", 9, timeOptions{}, 9},
		{"scale 9 micros", 9, timeOptions{maxTimestampPrecision: Microseconds}, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire arrow.Array
			values := []*time.Time{&testTime, &testTimeBeforeEpoch, nil}
			if tc.scale <= 7 {
				wire = scaledInts(mem, tc.scale, values...)
			} else {
				wire = epochFractions(mem, tc.scale, nil, values...)
			}
			defer wire.Release()

			for _, tz := range []string{"", "UTC"} { // NTZ, LTZ
				dt := &arrow.TimestampType{Unit: tc.opts.timestampUnit(tc.scale), TimeZone: tz}
				got, err := convertTimestamps(ctx, wire, dt, tc.scale, tc.opts.maxTimestampPrecision)
				require.NoError(t, err)
				assert.Equal(t, dt, got.DataType())
				// rounded down to the digits of the unit
				assert.Equal(t, []*time.Time{
					ref(truncate(truncate(testTime, tc.scale), tc.digits)),
					ref(truncate(truncate(testTimeBeforeEpoch, tc.scale), tc.digits)),
					nil,
				}, timestamps(t, got))
				got.Release()
			}
		})
	}
}

func ref[T any](v T) *T { return &v }

func TestConvertTimestampsOverflow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := testContext(mem)

	year1 := time.Date(1, 1, 1, 0, 0, 0, 500_000_000, time.UTC)
	wire := epochFractions(mem, 9, nil, &year1)
	defer wire.Release()

	_, err := convertTimestamps(ctx, wire, &arrow.TimestampType{Unit: arrow.Nanosecond}, 9, NanosecondsNoOverflow)
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusInvalidData, adbcErr.Code)

	got, err := convertTimestamps(ctx, wire, &arrow.TimestampType{Unit: arrow.Microsecond}, 9, Microseconds)
	require.NoError(t, err)
	defer got.Release()
	assert.Equal(t, []*time.Time{&year1}, timestamps(t, got))
}

func TestConvertTimestampTZs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := testContext(mem)

	offsets := []int32{330, -150, 0}
	for _, tc := range []struct {
		name   string
		scale  int64
		opts   timeOptions
		digits int64
	}{
		{"scale 0", 0, timeOptions{}, 0},
		{"scale 2", 2, timeOptions{}, 0},
		{"scale 2 lossless", 2, timeOptions{losslessTimeUnit: true}, 2},
		{"scale 3", 3, timeOptions{}, 3},
		{"scale 4", 4, timeOptions{}, 3},
		{"scale 8", 8, timeOptions{}, 6},
		{"scale 9", 9, timeOptions{}, 9},
		{"scale 9 micros", 9, timeOptions{maxTimestampPrecision: Microseconds}, 6},
	} {
		for _, withOffset := range []bool{false, true} {
			opts := tc.opts
			opts.timestampTZWithOffset = withOffset
			t.Run(tc.name, func(t *testing.T) {
				var wire arrow.Array
				values := []*time.Time{&testTime, &testTimeBeforeEpoch, nil}
				if tc.scale <= 3 {
					wire = scaledIntsWithOffsets(mem, tc.scale, offsets, values...)
				} else {
					wire = epochFractions(mem, tc.scale, offsets, values...)
				}
				defer wire.Release()

				dt := &arrow.TimestampType{Unit: opts.timestampUnit(tc.scale), TimeZone: "UTC"}
				got, err := convertTimestampTZs(ctx, wire, dt, tc.scale, opts)
				require.NoError(t, err)
				defer got.Release()
				assert.Equal(t, opts.timestampTZType(dt), got.DataType())

				want := []*time.Time{
					ref(truncate(truncate(testTime, tc.scale), tc.digits)),
					ref(truncate(truncate(testTimeBeforeEpoch, tc.scale), tc.digits)),
					nil,
				}
				if !withOffset {
					assert.Equal(t, want, timestamps(t, got))
					return
				}
				assertTimestampsWithOffset(t, got, want, []int16{330, -150, 0})
			})
		}
	}
}

func TestConvertTimes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := testContext(mem)

	// 23:59:59.987654321
	const nanos = int64(86_399_987_654_321)
	for _, tc := range []struct {
		scale int64
		// Snowflake sends Int32 up to scale 4, Int64 above
		wire arrow.DataType
		opts timeOptions
		want arrow.Array
	}{
		{0, arrow.PrimitiveTypes.Int32, timeOptions{}, arrayOf(mem, arrow.FixedWidthTypes.Time32s, 86_399)},
		{2, arrow.PrimitiveTypes.Int32, timeOptions{}, arrayOf(mem, arrow.FixedWidthTypes.Time32s, 86_399)},
		{2, arrow.PrimitiveTypes.Int32, timeOptions{losslessTimeUnit: true}, arrayOf(mem, arrow.FixedWidthTypes.Time32ms, 86_399_980)},
		{4, arrow.PrimitiveTypes.Int32, timeOptions{}, arrayOf(mem, arrow.FixedWidthTypes.Time32ms, 86_399_987)},
		{5, arrow.PrimitiveTypes.Int64, timeOptions{}, arrayOf(mem, arrow.FixedWidthTypes.Time32ms, 86_399_987)},
		{5, arrow.PrimitiveTypes.Int64, timeOptions{losslessTimeUnit: true}, arrayOf(mem, arrow.FixedWidthTypes.Time64us, 86_399_987_650)},
		{7, arrow.PrimitiveTypes.Int64, timeOptions{}, arrayOf(mem, arrow.FixedWidthTypes.Time64us, 86_399_987_654)},
		{8, arrow.PrimitiveTypes.Int64, timeOptions{losslessTimeUnit: true}, arrayOf(mem, arrow.FixedWidthTypes.Time64ns, 86_399_987_654_320)},
		{9, arrow.PrimitiveTypes.Int64, timeOptions{}, arrayOf(mem, arrow.FixedWidthTypes.Time64ns, nanos)},
	} {
		raw := nanos / pow10[9-tc.scale]
		wire := arrayOf(mem, tc.wire, raw)
		got, err := convertTimes(ctx, wire, tc.opts.timeType(tc.scale), tc.scale)
		require.NoError(t, err, "scale %d", tc.scale)
		assert.Truef(t, array.Equal(tc.want, got), "scale %d: got %s, want %s", tc.scale, got, tc.want)
		assert.True(t, got.IsNull(1))
		got.Release()
		wire.Release()
		tc.want.Release()
	}
}

// arrayOf builds an array of dt with the given value followed by a null.
func arrayOf(mem memory.Allocator, dt arrow.DataType, value int64) arrow.Array {
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	switch b := b.(type) {
	case *array.Int32Builder:
		b.Append(int32(value))
	case *array.Int64Builder:
		b.Append(value)
	case *array.Time32Builder:
		b.Append(arrow.Time32(value))
	case *array.Time64Builder:
		b.Append(arrow.Time64(value))
	}
	b.AppendNull()
	return b.NewArray()
}

func TestJSONTimestamps(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	opts := timeOptions{timestampTZWithOffset: true}
	snowflakeType := func(name string) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{MetadataKeySnowflakeType: name})
	}
	ntz3 := &arrow.TimestampType{Unit: opts.timestampUnit(3)}
	tz9 := &arrow.TimestampType{Unit: opts.timestampUnit(9), TimeZone: "UTC"}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ntz", Type: ntz3, Nullable: true, Metadata: snowflakeType("timestamp_ntz")},
		{Name: "tz", Type: opts.timestampTZType(tz9), Nullable: true, Metadata: snowflakeType("timestamp_tz")},
	}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	rec, err := jsonDataToArrow(context.Background(), bldr, [][]*string{
		{ref("1704164645.123000000"), ref("1704164645.123456789 1770")},
		{nil, nil},
	}, opts)
	require.NoError(t, err)
	defer rec.Release()

	assert.Equal(t, []*time.Time{ref(truncate(testTime, 3)), nil}, timestamps(t, rec.Column(0)))
	assertTimestampsWithOffset(t, rec.Column(1), []*time.Time{&testTime, nil}, []int16{330, 0})
}

// assertTimestampsWithOffset checks an arrow.timestamp_with_offset array: its
// values (nil for a null), offsets, and that the fields of its storage hold no
// nulls.
func assertTimestampsWithOffset(t *testing.T, got arrow.Array, want []*time.Time, wantOffsets []int16) {
	t.Helper()
	a, ok := got.(*extensions.TimestampWithOffsetArray)
	require.True(t, ok, "got %s", got.DataType())
	s := a.Storage().(*array.Struct)
	unit := s.Field(0).DataType().(*arrow.TimestampType).Unit
	assert.Equal(t, extensions.NewTimestampWithOffsetType(unit), a.DataType())
	assert.Zero(t, s.Field(0).NullN(), "timestamp nulls")
	assert.Zero(t, s.Field(1).NullN(), "offset_minutes nulls")
	ts := s.Field(0).(*array.Timestamp)
	offsets := s.Field(1).(*array.Int16)
	require.Equal(t, len(want), s.Len())
	for i := range want {
		if want[i] == nil {
			assert.True(t, s.IsNull(i), "row %d", i)
			assert.Zero(t, ts.Value(i), "row %d", i)
			assert.Zero(t, offsets.Value(i), "row %d", i)
			continue
		}
		assert.True(t, s.IsValid(i), "row %d", i)
		assert.Equal(t, *want[i], ts.Value(i).ToTime(unit), "row %d", i)
		assert.Equal(t, wantOffsets[i], offsets.Value(i), "row %d", i)
		// arrow-go reads the same instant and offset
		assert.True(t, want[i].Equal(a.Value(i)), "row %d", i)
		_, offset := a.Value(i).Zone()
		assert.Equal(t, int(wantOffsets[i])*60, offset, "row %d", i)
	}
}

func TestTimestampWithOffsetField(t *testing.T) {
	// Exported through the C Data Interface, the field carries the extension
	// name and metadata, so consumers see arrow.timestamp_with_offset.
	opts := timeOptions{timestampTZWithOffset: true}
	field := arrow.Field{Name: "tz", Type: opts.timestampTZType(&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}), Nullable: true}
	var c cdata.CArrowSchema
	cdata.ExportArrowSchema(arrow.NewSchema([]arrow.Field{field}, nil), &c)
	imported, err := cdata.ImportCArrowSchema(&c)
	require.NoError(t, err)
	// arrow-go's importer keeps the extension keys as metadata of the storage
	md := imported.Field(0).Metadata
	name, _ := md.GetValue(ipc.ExtensionTypeKeyName)
	assert.Equal(t, "arrow.timestamp_with_offset", name)
	serialized, ok := md.GetValue(ipc.ExtensionMetadataKeyName)
	assert.True(t, ok)
	assert.Equal(t, "", serialized)
	assert.Equal(t, extensions.NewTimestampWithOffsetType(arrow.Nanosecond).StorageType(), imported.Field(0).Type)

	// without the option, a plain timestamp
	dt := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	assert.Equal(t, dt, timeOptions{}.timestampTZType(dt))
}
