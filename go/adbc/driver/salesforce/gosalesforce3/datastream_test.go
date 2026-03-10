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

func TestGetDataStream(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Contains(t, r.URL.Path, "/data-streams/test_stream")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataStream{
			ID:   "abc123",
			Name: "test_stream",
		})
	}))

	ds, err := client.GetDataStream(context.Background(), "test_stream")
	require.NoError(t, err)
	assert.Equal(t, "test_stream", ds.Name)
	assert.Equal(t, "abc123", ds.ID)
}
