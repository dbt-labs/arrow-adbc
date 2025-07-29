package api

import (
	"time"
)

// AuthType represents the type of authentication flow
type AuthType int

const (
	AuthTypeJWT AuthType = iota
	AuthTypeUsernamePassword
	AuthTypeRefreshToken
)

// Token represents an authenticated token with expiry information
type Token struct {
	AccessToken  string
	RefreshToken string
	InstanceURL  string
	TokenType    string
	ExpiresAt    time.Time
}

// IsExpired checks if the token is expired
func (t *Token) IsExpired() bool {
	if t.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(t.ExpiresAt)
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	LoginURL     string
	ClientID     string
	ClientSecret string
	Username     string
	Password     string
	PrivateKey   string // PEM-encoded private key for JWT
	RefreshToken string
	Timeout      time.Duration
	MaxRetries   int
}

// DefaultAuthConfig returns a default authentication configuration
func DefaultAuthConfig() *AuthConfig {
	return &AuthConfig{
		LoginURL:   "https://login.salesforce.com",
		Timeout:    30 * time.Second,
		MaxRetries: 3,
	}
}

// AuthError represents an authentication-related error
type AuthError struct {
	Code    int
	Message string
	Type    string
}

func (e *AuthError) Error() string {
	return e.Message
}

// Common authentication errors
var (
	ErrInvalidCredentials = &AuthError{Code: 400, Message: "Invalid credentials", Type: "invalid_credentials"}
	ErrTokenExpired       = &AuthError{Code: 401, Message: "Token expired", Type: "token_expired"}
	ErrInvalidGrant       = &AuthError{Code: 400, Message: "Invalid grant", Type: "invalid_grant"}
	ErrInsufficientScope  = &AuthError{Code: 403, Message: "Insufficient scope", Type: "insufficient_scope"}
)

// SqlQueryRequest represents a SQL query request to Data Cloud
type SqlQueryRequest struct {
	SQL           string         `json:"sql"`
	RowLimit      *int64         `json:"rowLimit,omitempty"`
	SqlParameters []SqlParameter `json:"sqlParameters,omitempty"`
	Dataspace     string         `json:"dataspace,omitempty"`
	WorkloadName  string         `json:"workloadName,omitempty"`
}

// SqlParameter represents a parameter in a SQL query
type SqlParameter struct {
	Type  string      `json:"type"`
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

// SqlQueryResponse represents the response from a SQL query
type SqlQueryResponse struct {
	Data         [][]interface{}    `json:"data"`
	Metadata     []SqlQueryMetadata `json:"metadata"`
	Status       SqlQueryStatus     `json:"status"`
	ReturnedRows int64              `json:"returnedRows"`
}

// SqlQueryMetadata represents metadata for a SQL query result column
type SqlQueryMetadata struct {
	Name      string `json:"name"`
	Nullable  bool   `json:"nullable"`
	Type      string `json:"type"`
	Precision *int   `json:"precision,omitempty"`
	Scale     *int   `json:"scale,omitempty"`
}

// SqlQueryStatus represents the status of a SQL query execution
type SqlQueryStatus struct {
	ChunkCount       int     `json:"chunkCount"`
	CompletionStatus string  `json:"completionStatus"`
	ExpirationTime   string  `json:"expirationTime"`
	Progress         float64 `json:"progress"`
	QueryId          string  `json:"queryId"`
	RowCount         int64   `json:"rowCount"`
}

// QueryV2Request represents a SQL query request to the v2 query API
type QueryV2Request struct {
	Sql string `json:"sql"`
}

// QueryV2Response represents the response from the v2 query API
// reference: https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/data-cloud-query-api-reference/c360a-api-query-v2.html
type QueryV2Response struct {
	Data        [][]interface{}            `json:"data,omitempty"`
	Metadata    map[string]QueryV2Metadata `json:"metadata,omitempty"`
	Done        bool                       `json:"done"`
	NextBatchId *string                    `json:"nextBatchId,omitempty"`
	RowCount    int64                      `json:"rowCount,omitempty"`
	QueryId     string                     `json:"queryId,omitempty"`
	StartTime   string                     `json:"startTime,omitempty"`
	EndTime     string                     `json:"endTime,omitempty"`
	ArrowStream interface{}                `json:"arrowStream,omitempty"`
}

// QueryV2Metadata represents metadata for a v2 query result column
type QueryV2Metadata struct {
	Type         string `json:"type"`
	PlaceInOrder int    `json:"placeInOrder"`
	TypeCode     int    `json:"typeCode"`
}

// MetadataResponse represents the response from the metadata API
type MetadataResponse struct {
	Metadata []MetadataEntity `json:"metadata"`
}

// MetadataEntity represents a single metadata entity (table/object)
type MetadataEntity struct {
	Name                              string                 `json:"name"`
	DisplayName                       string                 `json:"displayName"`
	Category                          string                 `json:"category,omitempty"`
	Fields                            []MetadataField        `json:"fields,omitempty"`
	Indexes                           []interface{}          `json:"indexes,omitempty"`
	Relationships                     []MetadataRelationship `json:"relationships,omitempty"`
	PrimaryKeys                       []MetadataPrimaryKey   `json:"primaryKeys,omitempty"`
	ReferenceModelEntityDeveloperName string                 `json:"referenceModelEntityDeveloperName,omitempty"`
	Dimensions                        []MetadataDimension    `json:"dimensions,omitempty"`
	Measures                          []MetadataMeasure      `json:"measures,omitempty"`
	PartitionBy                       string                 `json:"partitionBy,omitempty"`
	LatestProcessTime                 string                 `json:"latestProcessTime,omitempty"`
	LatestSuccessfulProcessTime       string                 `json:"latestSuccessfulProcessTime,omitempty"`
}

// MetadataField represents a field/column in a metadata entity
type MetadataField struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName"`
	Type         string `json:"type"`
	KeyQualifier string `json:"keyQualifier,omitempty"`
	BusinessType string `json:"businessType,omitempty"`
	Precision    int    `json:"precision,omitempty"`
	Scale        int    `json:"scale,omitempty"`
	Nullable     bool   `json:"nullable,omitempty"`
}

// MetadataRelationship represents a relationship between entities
type MetadataRelationship struct {
	FromEntity          string `json:"fromEntity"`
	ToEntity            string `json:"toEntity"`
	FromEntityAttribute string `json:"fromEntityAttribute,omitempty"`
	ToEntityAttribute   string `json:"toEntityAttribute,omitempty"`
	Cardinality         string `json:"cardinality,omitempty"`
}

// MetadataPrimaryKey represents a primary key field
type MetadataPrimaryKey struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	IndexOrder  string `json:"indexOrder"`
}

// MetadataDimension represents a dimension in a calculated insight
type MetadataDimension struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName"`
	Type         string `json:"type"`
	BusinessType string `json:"businessType"`
}

// MetadataMeasure represents a measure in a calculated insight
type MetadataMeasure struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName"`
	Type         string `json:"type"`
	Rollupable   bool   `json:"rollupable"`
	BusinessType string `json:"businessType"`
}

// CreateJobRequest represents a request to create a data ingestion job
type CreateJobRequest struct {
	Object     string `json:"object"`
	SourceName string `json:"sourceName"`
	Operation  string `json:"operation"` // "upsert" or "delete"
}

// CreateJobResponse represents the response from creating a job
type CreateJobResponse struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	Object      string `json:"object"`
	Operation   string `json:"operation"`
	SourceName  string `json:"sourceName"`
	ContentType string `json:"contentType"`
	ContentURL  string `json:"contentUrl"`
}

// CloseJobRequest represents a request to close or abort a job
type CloseJobRequest struct {
	State string `json:"state"` // "UploadComplete" to close, "Aborted" to abort
}

// CloseJobResponse represents the response from closing or aborting a job
type CloseJobResponse struct {
	ID             string `json:"id"`
	Operation      string `json:"operation"`
	Object         string `json:"object"`
	CreatedById    string `json:"createdById"`
	CreatedDate    string `json:"createdDate"`
	SystemModstamp string `json:"systemModstamp"`
	State          string `json:"state"`
	ContentType    string `json:"contentType"`
	APIVersion     string `json:"apiVersion"`
}
