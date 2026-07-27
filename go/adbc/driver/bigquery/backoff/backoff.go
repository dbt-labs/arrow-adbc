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


// The backoff package implements a thread-safe global backoff object that consumes from
// a channel of rate-limit event timestamps and adjusts a the backoff time.
//
// This allows connections/statements to be aware of neighbors getting rate-limited, avoiding
// starvation and hangs.
//
// You can use `backoff.GetBackoff()` to get a Google-compatible `gax.Backoff` for a single request.
// At the end of each request, call `backoff.Update()` to consume from the rate-limit channel and
// adjust the rate-limiter up or down if necessary.
//
// Make sure to call `backoff.Setup()` to setup the channel listener.

package backoff

import (
	"math"
	"sync"
	"time"
)

type mode int8

const (
	rampUp mode = iota
	slowDown
)

type globalBackoff struct {
	rateLimitEvents chan time.Time
	lock            sync.RWMutex
	mode            mode
	lastModeChange  time.Time
	lastSleepTime   time.Duration

	RampUpAfter time.Duration
	Multiplier  float64
	Min         time.Duration
	Max         time.Duration
}

func (b *globalBackoff) Update(now time.Time) {
	b.lock.Lock()

	var epoch time.Time
	var latest time.Time
	for {
		timestamp, ok := <-b.rateLimitEvents
		if !ok {
			break
		}

		if timestamp.Compare(latest) > 0 {
			latest = timestamp
		}
	}

	if b.mode == slowDown && latest.Equal(epoch) {
		// we were slowing down but there are no new rate-limit events

		if b.lastModeChange.Add(b.RampUpAfter).Compare(now) < 0 {
			b.mode = rampUp
			b.lastModeChange = now
		}
	} else if b.mode == rampUp && latest != epoch {
		// we were ramping up but got a new rate-limit event

		b.mode = slowDown
		b.lastModeChange = now
	} else if b.mode == slowDown {
		// if slowing down, we need to register how much we slowed down to so that
		// when we start ramping up we can count down from that number

		b.lastSleepTime = b.currentSleepTime(now)
	}

	b.lock.Unlock()
}

func (b *globalBackoff) currentSleepTime(now time.Time) time.Duration {
	deltaSec := now.Sub(b.lastModeChange).Seconds()
	mult := math.Pow(b.Multiplier, deltaSec)

	var r time.Duration
	switch b.mode {
	case slowDown:
		r = time.Duration(min(b.Max.Nanoseconds(), int64(float64(b.Min.Nanoseconds())*mult)))
	case rampUp:
		r = time.Duration(max(b.Min.Nanoseconds(), int64(float64(b.lastSleepTime.Nanoseconds())/mult)))
	}
	return r
}

// CurrentSleepTime returns the current sleep time that should be applied to a single request
// according to the global rate-limiter
func (b *globalBackoff) CurrentSleepTime(now time.Time) time.Duration {
	b.lock.RLock()
	curr := b.currentSleepTime(now)
	b.lock.RUnlock()
	return curr
}

var backoff globalBackoff = globalBackoff{
	mode:        rampUp,
	RampUpAfter: 1 * time.Second,
	Multiplier:  1.3,
	Min:         50 * time.Millisecond,
	Max:         60 * time.Second,
}

// Setup makes the global backoff to listen for rate-limit events at the specificed channel
func Setup(c chan time.Time) {
	backoff.rateLimitEvents = c
}

// Update consumes from the channel and slows down or ramps up if necessary
func Update() {
	backoff.Update(time.Now())
}

// CurrentSleepTime returns the current sleep time given the global backoff
func CurrentSleepTime() time.Duration {
	return backoff.CurrentSleepTime(time.Now())
}
