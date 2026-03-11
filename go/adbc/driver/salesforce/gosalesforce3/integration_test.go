//go:build integration

package gosalesforce3

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"github.com/stretchr/testify/require"
)

func TestIntegration_AuthAndQuery(t *testing.T) {
	loginURL := os.Getenv("SFDC_LOGIN_URL")
	clientID := os.Getenv("SFDC_CLIENT_ID")
	username := os.Getenv("SFDC_USERNAME")
	keyPath := os.Getenv("SFDC_CLIENT_PRIVATE_KEY_PATH")

	if loginURL == "" || clientID == "" || username == "" || keyPath == "" {
		t.Skip("SFDC_ env vars not set, skipping integration test")
	}

	if !filepath.IsAbs(keyPath) {
		keyPath = filepath.Join("..", keyPath)
	}
	keyPEM, err := os.ReadFile(keyPath)
	require.NoError(t, err)

	client, err := NewClient(&types.AuthConfig{
		LoginURL:      loginURL,
		ClientID:      clientID,
		Username:      username,
		PrivateKeyPEM: string(keyPEM),
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Step 1: Authenticate
	err = client.Authenticate(ctx)
	require.NoError(t, err)
	t.Logf("Authenticated, instance: %s", client.instanceURL)

	// Step 2: Get metadata
	meta, err := client.GetMetadata(ctx, &types.MetadataRequest{})
	require.NoError(t, err)
	t.Logf("Metadata: %d entities", len(meta.Metadata))
	for i, e := range meta.Metadata {
		if i >= 5 {
			t.Logf("  ... and %d more", len(meta.Metadata)-5)
			break
		}
		t.Logf("  - %s (%s, %s)", e.Name, e.Category, e.EntityType)
	}

	// Step 3: Run a simple query if we have entities
	if len(meta.Metadata) > 0 {
		entity := meta.Metadata[0]
		sql := "SELECT * FROM " + entity.Name + " LIMIT 1"
		t.Logf("Query: %s", sql)

		resp, err := client.ExecuteSqlQuery(ctx, &types.SqlQueryRequest{
			SQL:      sql,
			RowLimit: 1,
		})
		require.NoError(t, err)
		t.Logf("Query returned %d rows, %d columns", resp.ReturnedRows, len(resp.Metadata))
		for _, col := range resp.Metadata {
			t.Logf("  - %s (%s)", col.Name, col.Type)
		}
	}
}
