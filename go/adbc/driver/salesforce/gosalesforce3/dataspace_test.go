package gosalesforce3

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpsertDataSpaceMembers(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Contains(t, r.URL.Path, "/data-spaces/default/members")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"Success": true,
			"Errors":  []string{},
		})
	}))

	members := []types.DataSpaceMember{
		{Name: "test_dlo"},
	}
	resp, err := client.UpsertDataSpaceMembers(context.Background(), "default", members)
	require.NoError(t, err)
	assert.True(t, resp.Success)
}
