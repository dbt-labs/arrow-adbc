package gosalesforce3

import (
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	cfg := &types.AuthConfig{
		LoginURL:      "https://login.salesforce.com",
		ClientID:      "test-client-id",
		Username:      "user@example.com",
		PrivateKeyPEM: "fake-key",
		APIVersion:    "v64.0",
	}
	client, err := NewClient(cfg)
	require.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()
}

func TestNewClient_NilConfig(t *testing.T) {
	_, err := NewClient(nil)
	assert.Error(t, err)
}

func TestNewClient_DefaultAPIVersion(t *testing.T) {
	cfg := &types.AuthConfig{
		LoginURL:      "https://login.salesforce.com",
		ClientID:      "test-client-id",
		Username:      "user@example.com",
		PrivateKeyPEM: "fake-key",
	}
	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer client.Close()
	assert.Contains(t, client.ssotBaseURL(), "/v64.0/")
}
