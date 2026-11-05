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

package snowflake_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Values of every TIME and TIMESTAMP_* scale, before and after the epoch.
var temporalTestValues = []time.Time{
	time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC),
	time.Date(1969, 12, 31, 23, 59, 59, 987654321, time.UTC),
}

// temporalQuery selects, for each value, TIME, TIMESTAMP_NTZ, TIMESTAMP_LTZ and
// TIMESTAMP_TZ (with a +05:30 offset) columns of the given scale, with the value
// truncated to the scale. Rows are ordered like temporalTestValues.
func temporalQuery(scale int) string {
	var rows []string
	for i, v := range temporalTestValues {
		v = v.Truncate(time.Duration(pow10(9 - scale)))
		layout := "2006-01-02 15:04:05"
		if scale > 0 {
			layout += "." + strings.Repeat("0", scale)
		}
		ts := v.Format(layout)
		tz := v.In(time.FixedZone("", 5*3600+1800)).Format(layout)
		rows = append(rows, fmt.Sprintf(
			"SELECT %d AS i, '%s'::TIME(%d) AS t, '%s'::TIMESTAMP_NTZ(%d) AS ntz, '%s +00:00'::TIMESTAMP_LTZ(%d) AS ltz, '%s +05:30'::TIMESTAMP_TZ(%d) AS tz",
			i, ts[11:], scale, ts, scale, ts, scale, tz, scale))
	}
	return strings.Join(rows, " UNION ALL ") + " ORDER BY i"
}

func pow10(n int) int64 {
	v := int64(1)
	for ; n > 0; n-- {
		v *= 10
	}
	return v
}

// queryTemporal runs temporalQuery(scale) with the given statement options.
func (suite *SnowflakeTests) queryTemporal(scale int, opts map[string]string) arrow.RecordBatch {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer stmt.Close()
	for k, v := range opts {
		suite.Require().NoError(stmt.SetOption(k, v))
	}
	suite.Require().NoError(stmt.SetSqlQuery(temporalQuery(scale)))
	rdr, _, err := stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err, "scale %d", scale)
	defer rdr.Release()
	suite.Require().True(rdr.Next(), "scale %d: %v", scale, rdr.Err())
	rec := rdr.RecordBatch()
	rec.Retain()
	suite.Require().False(rdr.Next())
	return rec
}

func (suite *SnowflakeTests) checkTemporal(rec arrow.RecordBatch, scale int, digits int) {
	msg := fmt.Sprintf("scale %d", scale)
	suite.Require().EqualValues(len(temporalTestValues), rec.NumRows(), msg)
	for row, v := range temporalTestValues {
		want := v.Truncate(time.Duration(pow10(9 - scale))).Truncate(time.Duration(pow10(9 - digits)))
		midnight := time.Date(want.Year(), want.Month(), want.Day(), 0, 0, 0, 0, time.UTC)

		var gotTime time.Duration
		switch t := rec.Column(1).(type) {
		case *array.Time32:
			gotTime = t.Value(row).ToTime(t.DataType().(*arrow.Time32Type).Unit).Sub(time.Unix(0, 0).UTC())
		case *array.Time64:
			gotTime = t.Value(row).ToTime(t.DataType().(*arrow.Time64Type).Unit).Sub(time.Unix(0, 0).UTC())
		}
		// TIME is not capped by the maximum timestamp precision
		wantTime := v.Truncate(time.Duration(pow10(9 - scale))).Sub(midnight)
		suite.Equal(wantTime.Truncate(time.Duration(pow10(9-3*(int(unitOf(rec.Column(1).DataType())))))), gotTime, "%s: TIME row %d", msg, row)

		for col := 2; col <= 3; col++ {
			ts := rec.Column(col).(*array.Timestamp)
			unit := ts.DataType().(*arrow.TimestampType).Unit
			suite.Equal(want, ts.Value(row).ToTime(unit), "%s: %s row %d", msg, rec.ColumnName(col), row)
		}

		ts := rec.Column(4).(*array.Timestamp)
		suite.Equal(want, ts.Value(row).ToTime(ts.DataType().(*arrow.TimestampType).Unit), "%s: TZ row %d", msg, row)
	}
}

func unitOf(dt arrow.DataType) arrow.TimeUnit {
	switch dt := dt.(type) {
	case *arrow.Time32Type:
		return dt.Unit
	case *arrow.Time64Type:
		return dt.Unit
	case *arrow.TimestampType:
		return dt.Unit
	}
	panic(dt)
}

// The default units drop the digits beyond scale / 3 * 3, but the values are
// otherwise exact.
func (suite *SnowflakeTests) TestTemporalScales() {
	for scale := 0; scale <= 9; scale++ {
		rec := suite.queryTemporal(scale, nil)
		suite.checkTemporal(rec, scale, scale/3*3)
		for col := 1; col <= 4; col++ {
			suite.Equal(arrow.TimeUnit(scale/3), unitOf(rec.Column(col).DataType()), "scale %d: %s", scale, rec.ColumnName(col))
		}
		rec.Release()
	}
}
