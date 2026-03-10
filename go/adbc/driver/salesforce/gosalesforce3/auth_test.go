package gosalesforce3

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateTestKey(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyBytes := x509.MarshalPKCS1PrivateKey(key)
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyBytes}
	return string(pem.EncodeToMemory(block))
}

func TestAuthenticate_JWTFlow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/services/oauth2/token", r.URL.Path)
		require.Equal(t, "POST", r.Method)

		err := r.ParseForm()
		require.NoError(t, err)
		assert.Equal(t, "urn:ietf:params:oauth:grant-type:jwt-bearer", r.FormValue("grant_type"))
		assert.NotEmpty(t, r.FormValue("assertion"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "test-access-token",
			"instance_url": "https://myinstance.salesforce.com",
			"token_type":   "Bearer",
		})
	}))
	defer server.Close()

	cfg := &types.AuthConfig{
		LoginURL:      server.URL,
		ClientID:      "test-client-id",
		Username:      "user@example.com",
		PrivateKeyPEM: generateTestKey(t),
		APIVersion:    "v64.0",
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer client.Close()

	err = client.Authenticate(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "https://myinstance.salesforce.com", client.instanceURL)
}

func TestAuthenticate_InvalidKey(t *testing.T) {
	cfg := &types.AuthConfig{
		LoginURL:      "https://login.salesforce.com",
		ClientID:      "test-client-id",
		Username:      "user@example.com",
		PrivateKeyPEM: "not-a-valid-key",
		APIVersion:    "v64.0",
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer client.Close()

	err = client.Authenticate(context.Background())
	assert.Error(t, err)
}
