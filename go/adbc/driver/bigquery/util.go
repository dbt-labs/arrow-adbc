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

package bigquery

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
)

var (
	// defaultRetryReasons are the reasons bigquery.Client retries for its
	// non-job API calls, such as tables.list:
	// https://github.com/googleapis/google-cloud-go/blob/bigquery/v1.85.0/bigquery/bigquery.go#L254-L255
	defaultRetryReasons = []string{"backendError", "rateLimitExceeded"}
)

// isRetryableError reports whether err is transient, retrying structured
// errors whose first reason is in retryableReasons.
func isRetryableError(err error, retryableReasons []string) bool {
	// Modeled on retryableError in bigquery.go:
	// https://github.com/googleapis/google-cloud-go/blob/bigquery/v1.85.0/bigquery/bigquery.go#L269-L325
	switch {
	case err == nil:
		return false
	case err == io.ErrUnexpectedEOF:
		return true
	case err.Error() == "http2: stream closed":
		return true
	}

	switch e := err.(type) {
	case *googleapi.Error:
		var reason string
		if len(e.Errors) > 0 {
			reason = e.Errors[0].Reason

			if slices.Contains(retryableReasons, reason) {
				return true
			}
		}

		if slices.Contains([]int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout}, e.Code) {
			return true
		}

	case *url.Error:
		for _, r := range []string{"connection refused", "connection reset"} {
			if strings.Contains(e.Error(), r) {
				return true
			}
		}

	case interface{ Temporary() bool }:
		if e.Temporary() {
			return true
		}
	}

	return isRetryableError(errors.Unwrap(err), retryableReasons)
}

// clientBackoff is the backoff bigquery.Client uses when retrying API calls:
// https://github.com/googleapis/google-cloud-go/blob/bigquery/v1.85.0/bigquery/bigquery.go#L238-L252
var clientBackoff = gax.Backoff{
	Initial:    1 * time.Second,
	Max:        32 * time.Second,
	Multiplier: 2,
}

// doWithRetry calls a generated REST API call
// https://github.com/googleapis/google-cloud-go/blob/bigquery/v1.85.0/bigquery/dataset.go#L658-L675
// https://github.com/googleapis/google-api-go-client/blob/v0.253.0/internal/gensupport/send.go#L47-L82
func doWithRetry[T any](ctx context.Context, do func(...googleapi.CallOption) (T, error), retryableReasons []string) (T, error) {
	var res T
	var lastErr error // lastErr is the raw error returned from the last call being retried
	err := gax.Invoke(ctx, func(context.Context, gax.CallSettings) error {
		res, lastErr = do()
		return lastErr
	}, gax.WithRetry(func() gax.Retryer {
		return gax.OnErrorFunc(clientBackoff, func(err error) bool {
			return isRetryableError(err, retryableReasons)
		})
	}))

	// err returned from gax.Invoke could be either a context end error (context.Canceled / DeadlineExceeded)
	// or a wrapped error if the lastErr is not retried
	// https://github.com/googleapis/google-cloud-go/blob/v0.123.0/internal/retry.go#L35-L55
	// https://github.com/googleapis/gax-go/blob/v2.15.0/v2/invoke.go#L95-L97
	if err != nil && lastErr != nil && !errors.Is(err, lastErr) {
		return res, errors.Join(err, lastErr)
	}
	return res, lastErr
}
