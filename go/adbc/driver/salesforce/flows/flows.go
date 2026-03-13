package flows

import (
	"context"
	"errors"
	"fmt"
	"time"

	sfapi "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce"
	sftypes "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/types"
)

// WaitForTransformReady polls a data transform until it leaves Processing status.
// If the transform enters Error status, it retries once before giving up.
func WaitForTransformReady(ctx context.Context, client *sfapi.Client, name string) (*sftypes.DataTransform, error) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	retried := false
	for {
		dt, err := client.GetDataTransform(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("waiting for transform ready: %w", err)
		}

		switch {
		case dt.Status.IsActive():
			return dt, nil
		case dt.Status.IsError():
			if !retried {
				retried = true
				if _, retryErr := client.RetryDataTransform(ctx, name); retryErr != nil {
					return nil, fmt.Errorf("retrying transform: %w", retryErr)
				}
				// Fall through to poll again
			} else {
				return nil, fmt.Errorf("transform %q in error state after retry", name)
			}
		case dt.Status.IsProcessing():
			// Keep polling
		default:
			return nil, fmt.Errorf("transform %q in unexpected status: %s", name, dt.Status)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// WaitForTransformDeleted polls until a data transform returns 404.
func WaitForTransformDeleted(ctx context.Context, client *sfapi.Client, name string) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		_, err := client.GetDataTransform(ctx, name)
		if err != nil {
			var sfErr *sfapi.SalesforceError
			if errors.As(err, &sfErr) && sfErr.IsNotFound() {
				return nil
			}
			return fmt.Errorf("waiting for transform deleted: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// WaitForTransformRun polls the run status of a data transform until it reaches a terminal state.
func WaitForTransformRun(ctx context.Context, client *sfapi.Client, name string) (*sftypes.DataTransform, error) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		if _, err := client.RefreshDataTransformStatus(ctx, name); err != nil {
			return nil, fmt.Errorf("refreshing transform status: %w", err)
		}

		dt, err := client.GetDataTransform(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("getting transform run status: %w", err)
		}

		if dt.LastRunStatus.IsTerminal() {
			return dt, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// RunAndWaitForTransform triggers a transform run and waits for it to complete.
func RunAndWaitForTransform(ctx context.Context, client *sfapi.Client, name string) (*sftypes.DataTransform, error) {
	if _, err := client.RunDataTransform(ctx, name); err != nil {
		return nil, fmt.Errorf("running transform: %w", err)
	}
	return WaitForTransformRun(ctx, client, name)
}

// WaitForDLOActive polls a data lake object until its status is Active.
func WaitForDLOActive(ctx context.Context, client *sfapi.Client, name string) (*sftypes.DataLakeObject, error) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		dlo, err := client.GetDataLakeObject(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("waiting for DLO active: %w", err)
		}

		if dlo.Status == "Active" {
			return dlo, nil
		}
		if dlo.Status == "Error" {
			return nil, fmt.Errorf("DLO %q entered error state", name)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// WaitForDLODeleted polls until a data lake object returns 404.
func WaitForDLODeleted(ctx context.Context, client *sfapi.Client, name string) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		_, err := client.GetDataLakeObject(ctx, name)
		if err != nil {
			var sfErr *sfapi.SalesforceError
			if errors.As(err, &sfErr) && sfErr.IsNotFound() {
				return nil
			}
			return fmt.Errorf("waiting for DLO deleted: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// DeleteDLOAndWait deletes a data lake object and polls until it is gone.
func DeleteDLOAndWait(ctx context.Context, client *sfapi.Client, name string) error {
	err := client.DeleteDataLakeObject(ctx, name)
	if err != nil {
		var sfErr *sfapi.SalesforceError
		if errors.As(err, &sfErr) && sfErr.IsNotFound() {
			return nil
		}
		return fmt.Errorf("deleting DLO: %w", err)
	}
	return WaitForDLODeleted(ctx, client, name)
}

// DeleteTransformAndWait cancels any in-progress run, deletes the transform,
// and polls until it is gone.
func DeleteTransformAndWait(ctx context.Context, client *sfapi.Client, name string) error {
	// Cancel unconditionally — a cancel on a non-running transform is a noop
	// (may return an error, which we ignore).
	_, _ = client.CancelDataTransform(ctx, name)

	// Wait for any in-progress run to finish before deleting.
	// The transform must be in a stable state (Active/Error) to delete.
	dt, err := client.GetDataTransform(ctx, name)
	if err != nil {
		var sfErr *sfapi.SalesforceError
		if errors.As(err, &sfErr) && sfErr.IsNotFound() {
			return nil
		}
		return fmt.Errorf("getting transform before delete: %w", err)
	}
	if dt.Status.IsProcessing() {
		if _, err := WaitForTransformReady(ctx, client, name); err != nil {
			return fmt.Errorf("waiting for transform to stabilize before delete: %w", err)
		}
	}

	err = client.DeleteDataTransform(ctx, name)
	if err != nil {
		var sfErr *sfapi.SalesforceError
		if errors.As(err, &sfErr) && sfErr.IsNotFound() {
			return nil
		}
		return fmt.Errorf("deleting transform: %w", err)
	}
	return WaitForTransformDeleted(ctx, client, name)
}

// ValidateAndCreateTransform validates a transform request, extracts the output data objects
// from the validation response, marks the primary key field, and creates the transform.
func ValidateAndCreateTransform(ctx context.Context, client *sfapi.Client, req *sftypes.DataTransformRequest, primaryKeyFieldName string) (*sftypes.DataTransform, error) {
	validation, err := client.ValidateDataTransform(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("validating transform: %w", err)
	}

	var collected sftypes.DataTransformOutputDataObjects
	for _, objects := range validation.OutputDataObjects {
		for i := range objects {
			// Set defaults the validation endpoint doesn't provide but the create endpoint requires.
			if objects[i].Label == "" {
				objects[i].Label = objects[i].Name
			}
			if objects[i].Category == "" {
				objects[i].Category = "Profile"
			}
			for j := range objects[i].Fields {
				if objects[i].Fields[j].Name == primaryKeyFieldName {
					objects[i].Fields[j].IsPrimaryKey = true
				}
				if objects[i].Fields[j].Label == "" {
					objects[i].Fields[j].Label = objects[i].Fields[j].Name
				}
			}
			collected = append(collected, objects[i])
		}
	}

	req.Definition.OutputDataObjects = collected

	return client.CreateDataTransform(ctx, req)
}
