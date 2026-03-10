package types

type SqlType = string

// Values match what Salesforce API returns (mixed case).
const (
	SqlTypeVarchar     SqlType = "Varchar"
	SqlTypeChar        SqlType = "Char"
	SqlTypeBigInt      SqlType = "BigInt"
	SqlTypeInteger     SqlType = "Integer"
	SqlTypeSmallInt    SqlType = "SmallInt"
	SqlTypeDouble      SqlType = "Double"
	SqlTypeFloat       SqlType = "Float"
	SqlTypeNumeric     SqlType = "Numeric"
	SqlTypeBool        SqlType = "Bool"
	SqlTypeDate        SqlType = "Date"
	SqlTypeTime        SqlType = "Time"
	SqlTypeTimestamp   SqlType = "Timestamp"
	SqlTypeTimestampTZ SqlType = "TimestampTZ"
	SqlTypeOid         SqlType = "Oid"
	SqlTypeUnspecified SqlType = "Unspecified"
)

type SqlQueryStatus struct {
	ChunkCount       int     `json:"chunkCount"`
	CompletionStatus string  `json:"completionStatus"`
	Progress         float64 `json:"progress"`
	QueryID          string  `json:"queryId"`
	RowCount         int64   `json:"rowCount"`
}

type SqlParameter struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	Value any    `json:"value"`
}

type SqlQueryRequest struct {
	SQL           string         `json:"sql"`
	RowLimit      int64          `json:"rowLimit,omitempty"`
	SqlParameters []SqlParameter `json:"sqlParameters,omitempty"`
	Dataspace     string         `json:"dataspace,omitempty"`
	WorkloadName  string         `json:"workloadName,omitempty"`
}

type SqlQueryResponse struct {
	Data         [][]any            `json:"data"`
	Metadata     []SqlQueryMetadata `json:"metadata"`
	Status       SqlQueryStatus     `json:"status"`
	ReturnedRows int64              `json:"returnedRows"`
}

type SqlQueryMetadata struct {
	Name      string  `json:"name"`
	Nullable  bool    `json:"nullable"`
	Type      SqlType `json:"type"`
	Precision *int    `json:"precision,omitempty"`
	Scale     *int    `json:"scale,omitempty"`
}
