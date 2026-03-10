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

func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	restyClient := resty.New()
	restyClient.SetHeader("Content-Type", "application/json")
	t.Cleanup(func() { restyClient.Close() })

	client, err := NewClient(&types.AuthConfig{APIVersion: "v64.0"}, WithHTTPClient(restyClient))
	require.NoError(t, err)
	client.instanceURL = server.URL
	client.tokenSource = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test"})
	return client
}

func TestCreateDataLakeObject(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/data-lake-objects")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataLakeObject{
			Name:   "test_dlo",
			Label:  "Test DLO",
			Status: "Active",
		})
	}))

	dlo, err := client.CreateDataLakeObject(context.Background(), &types.CreateDataLakeObjectRequest{
		Name:     "test_dlo",
		Label:    "Test DLO",
		Category: types.DLOCategoryProfile,
	})
	require.NoError(t, err)
	assert.Equal(t, "test_dlo", dlo.Name)
	assert.True(t, dlo.IsActive())
}

func TestGetDataLakeObject(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Contains(t, r.URL.Path, "/data-lake-objects/test_dlo")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataLakeObject{Name: "test_dlo", Status: "Active"})
	}))

	dlo, err := client.GetDataLakeObject(context.Background(), "test_dlo")
	require.NoError(t, err)
	assert.Equal(t, "test_dlo", dlo.Name)
}

func TestDeleteDataLakeObject(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		w.WriteHeader(http.StatusNoContent)
	}))

	err := client.DeleteDataLakeObject(context.Background(), "test_dlo")
	require.NoError(t, err)
}
