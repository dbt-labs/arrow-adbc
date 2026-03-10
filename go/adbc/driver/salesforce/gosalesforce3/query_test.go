package gosalesforce3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"resty.dev/v3"
)

func TestExecuteSqlQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/services/data/v64.0/ssot/query-sql", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.SqlQueryResponse{
			Data: [][]any{{"id1", "Test Account"}},
			Metadata: []types.SqlQueryMetadata{
				{Name: "Id", Type: "Varchar", Nullable: false},
				{Name: "Name", Type: "Varchar", Nullable: true},
			},
			ReturnedRows: 1,
		})
	}))
	defer server.Close()

	restyClient := resty.New()
	restyClient.SetHeader("Content-Type", "application/json")
	defer restyClient.Close()

	client, err := NewClient(&types.AuthConfig{APIVersion: "v64.0"}, WithHTTPClient(restyClient))
	require.NoError(t, err)
	client.instanceURL = server.URL
	client.tokenSource = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test"})

	resp, err := client.ExecuteSqlQuery(context.Background(), &types.SqlQueryRequest{
		SQL:      "SELECT Id, Name FROM Account LIMIT 1",
		RowLimit: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.ReturnedRows)
	assert.Len(t, resp.Metadata, 2)
}
