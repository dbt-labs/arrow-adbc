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

func TestGetMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/services/data/v64.0/ssot/metadata", r.URL.Path)
		assert.Equal(t, "GET", r.Method)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.MetadataResponse{
			Metadata: []types.MetadataEntity{
				{Name: "Account", Label: "Account", Category: "Profile"},
			},
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

	resp, err := client.GetMetadata(context.Background(), &types.MetadataRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.Metadata, 1)
	assert.Equal(t, "Account", resp.Metadata[0].Name)
}

func TestGetMetadata_WithFilters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "default", r.URL.Query().Get("dataspace"))
		assert.Equal(t, "Profile", r.URL.Query().Get("entityCategory"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.MetadataResponse{Metadata: []types.MetadataEntity{}})
	}))
	defer server.Close()

	restyClient := resty.New()
	restyClient.SetHeader("Content-Type", "application/json")
	defer restyClient.Close()

	client, err := NewClient(&types.AuthConfig{APIVersion: "v64.0"}, WithHTTPClient(restyClient))
	require.NoError(t, err)
	client.instanceURL = server.URL
	client.tokenSource = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test"})

	resp, err := client.GetMetadata(context.Background(), &types.MetadataRequest{
		Dataspace:      "default",
		EntityCategory: "Profile",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}
