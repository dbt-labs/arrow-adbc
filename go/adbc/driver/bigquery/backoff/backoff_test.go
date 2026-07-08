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

package backoff

import (
	"testing"
	"time"
)

var baseTime = time.Unix(1_700_000_000, 0)

func newBackoff() *globalBackoff {
	return &globalBackoff{
		mode:        rampUp,
		RampUpAfter: 1 * time.Second,
		Multiplier:  1.3,
		Min:         50 * time.Millisecond,
		Max:         60 * time.Second,
	}
}

func closedChan(events ...time.Time) chan time.Time {
	ch := make(chan time.Time, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	return ch
}

func TestCurrentSleepTimeSlowDownGrowsAndClamps(t *testing.T) {
	b := newBackoff()
	b.mode = slowDown
	b.lastModeChange = baseTime

	elapsed := []time.Duration{0, 1 * time.Second, 5 * time.Second, 30 * time.Second}

	var prev time.Duration
	for i, d := range elapsed {
		got := b.CurrentSleepTime(baseTime.Add(d))

		if got < b.Min || got > b.Max {
			t.Errorf("elapsed %v: sleep %v out of bounds [%v, %v]", d, got, b.Min, b.Max)
		}
		if i > 0 && got <= prev {
			t.Errorf("elapsed %v: sleep %v not greater than previous %v", d, got, prev)
		}
		prev = got
	}

	if got := b.CurrentSleepTime(baseTime.Add(1000 * time.Second)); got != b.Max {
		t.Errorf("large elapsed: sleep %v, want clamp to Max %v", got, b.Max)
	}
}

func TestCurrentSleepTimeRampUpShrinksAndClamps(t *testing.T) {
	b := newBackoff()
	b.lastSleepTime = b.Max
	b.lastModeChange = baseTime

	elapsed := []time.Duration{0, 1 * time.Second, 5 * time.Second, 30 * time.Second}

	var prev time.Duration
	for i, d := range elapsed {
		got := b.CurrentSleepTime(baseTime.Add(d))

		if got < b.Min || got > b.Max {
			t.Errorf("elapsed %v: sleep %v out of bounds [%v, %v]", d, got, b.Min, b.Max)
		}
		if i > 0 && got >= prev {
			t.Errorf("elapsed %v: sleep %v not less than previous %v", d, got, prev)
		}
		prev = got
	}

	if got := b.CurrentSleepTime(baseTime.Add(1000 * time.Second)); got != b.Min {
		t.Errorf("large elapsed: sleep %v, want clamp to Min %v", got, b.Min)
	}

	b.lastSleepTime = 0
	if got := b.CurrentSleepTime(baseTime); got != b.Min {
		t.Errorf("zero lastSleepTime: sleep %v, want Min %v", got, b.Min)
	}
}

func TestUpdateRampUpToSlowDown(t *testing.T) {
	b := newBackoff()
	b.rateLimitEvents = closedChan(baseTime)

	b.Update(baseTime)

	if b.mode != slowDown {
		t.Errorf("mode = %v, want slowDown", b.mode)
	}
	if !b.lastModeChange.Equal(baseTime) {
		t.Errorf("lastModeChange = %v, want %v", b.lastModeChange, baseTime)
	}
}

func TestUpdateSlowDownRecordsLastSleepTime(t *testing.T) {
	b := newBackoff()
	b.mode = slowDown
	b.lastModeChange = baseTime
	b.rateLimitEvents = closedChan(baseTime)

	b.Update(baseTime.Add(5 * time.Second))

	if b.mode != slowDown {
		t.Errorf("mode = %v, want slowDown", b.mode)
	}
	if b.lastSleepTime <= b.Min {
		t.Errorf("lastSleepTime %v not greater than Min %v", b.lastSleepTime, b.Min)
	}
}

func TestUpdateSlowDownStaysWhenNotEnoughTime(t *testing.T) {
	b := newBackoff()
	b.mode = slowDown
	b.lastModeChange = baseTime
	b.rateLimitEvents = closedChan()

	// now is within RampUpAfter of lastModeChange, so no transition.
	b.Update(baseTime.Add(500 * time.Millisecond))

	if b.mode != slowDown {
		t.Errorf("mode = %v, want slowDown (RampUpAfter not elapsed)", b.mode)
	}
}

func TestUpdateSlowDownToRampUp(t *testing.T) {
	b := newBackoff()
	b.mode = slowDown
	b.lastModeChange = baseTime
	b.rateLimitEvents = closedChan()

	now := baseTime.Add(2 * time.Second)
	b.Update(now)

	if b.mode != rampUp {
		t.Errorf("mode = %v, want rampUp", b.mode)
	}
	if !b.lastModeChange.Equal(now) {
		t.Errorf("lastModeChange = %v, want %v", b.lastModeChange, now)
	}
}

func TestUpdateFullCycle(t *testing.T) {
	b := newBackoff()
	now := baseTime

	// Start fresh: rampUp mode, sleep floored at Min.
	if b.mode != rampUp {
		t.Fatalf("initial mode = %v, want rampUp", b.mode)
	}
	if got := b.CurrentSleepTime(now); got != b.Min {
		t.Fatalf("initial sleep %v, want Min %v", got, b.Min)
	}

	// Ramp up (backoff grows): first event switches to slowDown.
	b.rateLimitEvents = closedChan(now)
	b.Update(now)
	if b.mode != slowDown {
		t.Fatalf("after first event: mode = %v, want slowDown", b.mode)
	}

	// Time elapses while still receiving events; slowDown records the grown
	// sleep time.
	now = now.Add(5 * time.Second)
	b.rateLimitEvents = closedChan(now)
	b.Update(now)
	if b.mode != slowDown {
		t.Fatalf("after second event: mode = %v, want slowDown", b.mode)
	}
	grown := b.CurrentSleepTime(now)
	if grown <= b.Min {
		t.Fatalf("slowDown sleep %v not greater than Min %v", grown, b.Min)
	}

	// Ramp down (backoff shrinks): events stop and RampUpAfter elapses.
	now = now.Add(2 * time.Second)
	b.rateLimitEvents = closedChan()
	b.Update(now)
	if b.mode != rampUp {
		t.Fatalf("after quiet period: mode = %v, want rampUp", b.mode)
	}
	// Sleep shrinks as time advances toward Min.
	near := b.CurrentSleepTime(now)
	far := b.CurrentSleepTime(now.Add(30 * time.Second))
	if far >= near {
		t.Fatalf("rampUp sleep did not shrink: near=%v far=%v", near, far)
	}
	if far < b.Min {
		t.Fatalf("rampUp sleep %v below Min %v", far, b.Min)
	}

	// Ramp up again: a new event switches back to slowDown.
	now = now.Add(1 * time.Second)
	b.rateLimitEvents = closedChan(now)
	b.Update(now)
	if b.mode != slowDown {
		t.Fatalf("after resumed events: mode = %v, want slowDown", b.mode)
	}
}
