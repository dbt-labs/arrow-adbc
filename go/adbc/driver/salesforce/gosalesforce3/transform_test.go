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

func TestCreateDataTransform(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/data-transforms")
		assert.NotContains(t, r.URL.Path, "/data-transforms-validation")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataTransform{
			Name:   "test_transform",
			Status: types.TransformStatusActive,
		})
	}))

	dt, err := client.CreateDataTransform(context.Background(), &types.CreateDataTransformRequest{
		Name: "test_transform",
	})
	require.NoError(t, err)
	assert.Equal(t, "test_transform", dt.Name)
	assert.True(t, dt.IsActive())
}

func TestGetDataTransform(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Contains(t, r.URL.Path, "/data-transforms/test_transform")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataTransform{
			Name:   "test_transform",
			Status: types.TransformStatusActive,
		})
	}))

	dt, err := client.GetDataTransform(context.Background(), "test_transform")
	require.NoError(t, err)
	assert.Equal(t, "test_transform", dt.Name)
}

func TestUpdateDataTransform(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Contains(t, r.URL.Path, "/data-transforms/test_transform")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataTransform{
			Name:   "test_transform",
			Label:  "Updated Label",
			Status: types.TransformStatusActive,
		})
	}))

	dt, err := client.UpdateDataTransform(context.Background(), &types.CreateDataTransformRequest{
		Name:  "test_transform",
		Label: "Updated Label",
	})
	require.NoError(t, err)
	assert.Equal(t, "test_transform", dt.Name)
	assert.Equal(t, "Updated Label", dt.Label)
}

func TestDeleteDataTransform(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Contains(t, r.URL.Path, "/data-transforms/test_transform")
		w.WriteHeader(http.StatusNoContent)
	}))

	err := client.DeleteDataTransform(context.Background(), "test_transform")
	require.NoError(t, err)
}

func TestValidateDataTransform(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/data-transforms-validation")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataTransformValidation{Valid: true})
	}))

	validation, err := client.ValidateDataTransform(context.Background(), &types.CreateDataTransformRequest{
		Name: "test_transform",
	})
	require.NoError(t, err)
	assert.True(t, validation.Valid)
	assert.Empty(t, validation.Errors)
}

func TestRunDataTransform(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/data-transforms/test_transform/actions/run")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataCloudActionResponse{Success: true})
	}))

	resp, err := client.RunDataTransform(context.Background(), "test_transform")
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestCancelDataTransform(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/data-transforms/test_transform/actions/cancel")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataCloudActionResponse{Success: true})
	}))

	resp, err := client.CancelDataTransform(context.Background(), "test_transform")
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestRefreshDataTransformStatus(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/data-transforms/test_transform/actions/refresh-status")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.DataCloudActionResponse{Success: true})
	}))

	resp, err := client.RefreshDataTransformStatus(context.Background(), "test_transform")
	require.NoError(t, err)
	assert.True(t, resp.Success)
}
