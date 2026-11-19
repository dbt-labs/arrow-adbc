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
	"github.com/apache/arrow-go/v18/arrow/compute"
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

	s, ms, us, ns := arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond
	for scale, want := range [][2]arrow.TimeUnit{
		// defaults, micros
		{s, s}, {s, s}, {s, s},
		{ms, ms}, {ms, ms}, {ms, ms},
		{us, us}, {us, us}, {us, us},
		{ns, us},
	} {
		got := [2]arrow.TimeUnit{defaults.timestampUnit(int64(scale)), micros.timestampUnit(int64(scale))}
		assert.Equal(t, want, got, "scale %d", scale)
	}

	// TIME is not capped by the maximum timestamp precision
	assert.Equal(t, arrow.FixedWidthTypes.Time64ns, micros.timeType(9))
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
		{"scale 3", 3, timeOptions{}, 3},
		{"scale 4", 4, timeOptions{}, 3},
		{"scale 6", 6, timeOptions{}, 6},
		{"scale 7", 7, timeOptions{}, 6},
		{"scale 8", 8, timeOptions{}, 6},
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
		{"scale 4", 4, timeOptions{}, 3},
		{"scale 8", 8, timeOptions{}, 6},
		{"scale 9", 9, timeOptions{}, 9},
		{"scale 9 micros", 9, timeOptions{maxTimestampPrecision: Microseconds}, 6},
	} {
		opts := tc.opts
		t.Run(tc.name, func(t *testing.T) {
			wire := epochFractions(mem, tc.scale, offsets, &testTime, &testTimeBeforeEpoch, nil)
			defer wire.Release()

			dt := &arrow.TimestampType{Unit: opts.timestampUnit(tc.scale), TimeZone: "UTC"}
			got, err := convertTimestampTZs(ctx, wire, dt, tc.scale, opts)
			require.NoError(t, err)
			defer got.Release()
			assert.Equal(t, dt, got.DataType())

			assert.Equal(t, []*time.Time{
				ref(truncate(truncate(testTime, tc.scale), tc.digits)),
				ref(truncate(truncate(testTimeBeforeEpoch, tc.scale), tc.digits)),
				nil,
			}, timestamps(t, got))
		})
	}
}
