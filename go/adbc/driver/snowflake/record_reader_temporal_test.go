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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

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
