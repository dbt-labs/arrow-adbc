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
	"strconv"

	"cloud.google.com/go/bigquery"

	"github.com/apache/arrow-go/v18/arrow"
)

// BIGQUERY:* schema-metadata keys — kept in sync with fs's
// crates/dbt-adapter/src/record_batch.rs.
const (
	MetadataKeyBigqueryQueryID             = "BIGQUERY:query_id"
	MetadataKeyBigqueryProjectID           = "BIGQUERY:project_id"
	MetadataKeyBigqueryLocation            = "BIGQUERY:location"
	MetadataKeyBigqueryStatementType       = "BIGQUERY:statement_type"
	MetadataKeyBigqueryNumDMLAffectedRows  = "BIGQUERY:num_dml_affected_rows"
	MetadataKeyBigqueryTotalBytesProcessed = "BIGQUERY:total_bytes_processed"
	MetadataKeyBigqueryTotalBytesBilled    = "BIGQUERY:total_bytes_billed"
	MetadataKeyBigquerySlotMillis          = "BIGQUERY:slot_ms"
)

// jobStats holds post-execution statistics from a BigQuery job. Identity
// fields (JobID / ProjectID / Location) come straight from the *Job; the
// rest come from QueryStatistics and are zero/empty when the job did not
// produce query statistics (e.g. non-query jobs, or when the caller opted
// out of stats fetching).
type jobStats struct {
	JobID              string
	ProjectID          string
	Location           string
	StatementType      string
	NumDMLAffectedRows int64
	BytesProcessed     int64
	BytesBilled        int64
	SlotMillis         int64
}

func newJobStats(job *bigquery.Job) jobStats {
	return jobStats{
		JobID:     job.ID(),
		ProjectID: job.ProjectID(),
		Location:  job.Location(),
	}
}

// fetch pulls the latest server-side JobStatus (one extra API call) and
// populates the stats. Errors are swallowed: stats are best-effort so they
// never fail query execution.
func (s *jobStats) fetch(ctx context.Context, job *bigquery.Job) {
	status, err := job.Status(ctx)
	if err != nil || status == nil {
		return
	}
	s.fromStatus(ctx, status)
}

// fromStatus populates the stats from an already-fetched JobStatus.
func (s *jobStats) fromStatus(ctx context.Context, status *bigquery.JobStatus) {
	if status.Statistics == nil {
		return
	}
	s.BytesProcessed = status.Statistics.TotalBytesProcessed
	qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		return
	}
	s.BytesBilled = qs.TotalBytesBilled
	s.NumDMLAffectedRows = qs.NumDMLAffectedRows
	s.StatementType = qs.StatementType
	s.SlotMillis = qs.SlotMillis
	if qs.TotalBytesProcessed != 0 {
		s.BytesProcessed = qs.TotalBytesProcessed
	}

	// For CREATE_TABLE_AS_SELECT it additionally issues a get_table call on
	// the DDL target so NumDMLAffectedRows carries the destination row count,
	// BigQuery does not populate NumDMLAffectedRows for DDL.
	// reference: https://github.com/dbt-labs/dbt-adapters/blob/9fce78f44db248ba33832c0f65c884a5139c0169/dbt-bigquery/src/dbt/adapters/bigquery/connections.py#L345-L346
	if qs.StatementType == "CREATE_TABLE_AS_SELECT" && qs.DDLTargetTable != nil {
		if md, err := qs.DDLTargetTable.Metadata(ctx); err == nil {
			s.NumDMLAffectedRows = int64(md.NumRows)
		}
	}
}

// attachToSchema returns a new schema with BIGQUERY:* metadata keys added
// for every populated stat. When called with a nil receiver the schema is
// returned unchanged (aside from a defensive metadata copy) — this lets
// callers pass through opt-out queries without a branch.
//
// Numeric fields are emitted even when zero so consumers can distinguish
// "stat available, value is 0" from "stat not provided".
func (s *jobStats) attachToSchema(schema *arrow.Schema) *arrow.Schema {
	meta := schema.Metadata().ToMap()
	if s == nil {
		finalMeta := arrow.MetadataFrom(meta)
		return arrow.NewSchema(schema.Fields(), &finalMeta)
	}
	if s.JobID != "" {
		meta[MetadataKeyBigqueryQueryID] = s.JobID
	}
	if s.ProjectID != "" {
		meta[MetadataKeyBigqueryProjectID] = s.ProjectID
	}
	if s.Location != "" {
		meta[MetadataKeyBigqueryLocation] = s.Location
	}
	if s.StatementType != "" {
		meta[MetadataKeyBigqueryStatementType] = s.StatementType
	}
	meta[MetadataKeyBigqueryNumDMLAffectedRows] = strconv.FormatInt(s.NumDMLAffectedRows, 10)
	meta[MetadataKeyBigqueryTotalBytesProcessed] = strconv.FormatInt(s.BytesProcessed, 10)
	meta[MetadataKeyBigqueryTotalBytesBilled] = strconv.FormatInt(s.BytesBilled, 10)
	meta[MetadataKeyBigquerySlotMillis] = strconv.FormatInt(s.SlotMillis, 10)
	finalMeta := arrow.MetadataFrom(meta)
	return arrow.NewSchema(schema.Fields(), &finalMeta)
}
