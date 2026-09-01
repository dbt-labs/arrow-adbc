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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// runWithTimeout runs fn on its own goroutine and reports whether it
// returned within the given timeout. It does not (and cannot) kill fn if it
// hangs; the goroutine is simply abandoned when the test process exits.
func runWithTimeout(fn func(), timeout time.Duration) (returned bool) {
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func emptyRecordBatch() arrow.RecordBatch {
	return array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
}

// Regression test: Release() ranges over every entry in r.chs, so a nil
// slot (chs[1] here) hangs it forever, same as Next().
func TestReader_Release_NilChannelSlot(t *testing.T) {
	ch0 := make(chan arrow.RecordBatch)
	close(ch0) // batch 0 already fully drained

	done := make(chan struct{})
	close(done) // simulate: all producer goroutines have already finished

	r := &reader{
		refCount: 1,
		chs:      []chan arrow.RecordBatch{ch0, nil}, // chs[1] "not created yet"
		cancelFn: func() {},
		done:     done,
	}

	if returned := runWithTimeout(r.Release, 500*time.Millisecond); !returned {
		t.Fatal("Release() did not return: blocked receiving from a nil channel slot (chs[1])")
	}
}

// Documents the same hazard in Next(); unlike Release() above, this
// doesn't change behavior across the port since Next() itself isn't
// touched by the fix.
func TestReader_Next_NilChannelSlot(t *testing.T) {
	ch0 := make(chan arrow.RecordBatch, 1)
	ch0 <- emptyRecordBatch()
	close(ch0)

	r := &reader{
		chs: []chan arrow.RecordBatch{ch0, nil}, // chs[1] "not created yet"
	}

	// First call: drains the one buffered record straight off chs[0] and
	// returns before curChIndex ever advances. Must not hang.
	if returned := runWithTimeout(func() { r.Next() }, 200*time.Millisecond); !returned {
		t.Fatal("first Next() call unexpectedly hung; should have returned the buffered record from chs[0]")
	}
	if r.rec == nil {
		t.Fatal("expected first Next() call to have consumed the buffered record from chs[0]")
	}

	// Second call: chs[0] is now closed/drained, so this iteration gets
	// ok=false, increments curChIndex to 1, and tries chs[1] -- which is
	// nil. That's the hang.
	if returned := runWithTimeout(func() { r.Next() }, 200*time.Millisecond); returned {
		t.Fatal("second Next() call unexpectedly returned; expected it to hang on the nil channel at chs[1] " +
			"(if this starts passing, something changed Next()'s nil-channel handling, not just newRecordReader's construction order)")
	}
}
