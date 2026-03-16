package flows

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	sfapi "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api"
	sftypes "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/cenkalti/backoff/v5"
)

// WaitForTransformReady polls a data transform until it leaves Processing status.
// If the transform enters Error status, it retries once before giving up.
func WaitForTransformReady(ctx context.Context, client *sfapi.Client, name string) (*sftypes.DataTransform, error) {

	return backoff.Retry(
		ctx,
		func() (*sftypes.DataTransform, error) {
			dt, err := client.GetDataTransform(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("waiting for transform ready: %w", err)
			}

			switch {
			case dt.Status.IsProcessing():
				return dt, fmt.Errorf("data transform still processing")
			case dt.Status.IsDeleting():
				return dt, backoff.Permanent(fmt.Errorf("data transform is being deleted"))
			case dt.Status.IsActive():
				return dt, nil
			case dt.Status.IsError():
				return dt, nil
			default:
				return nil, backoff.Permanent(fmt.Errorf("something went wrong. unknown data transform status %q", dt.Status))
			}
		},
		backoff.WithNotify(func(err error, dur time.Duration) {
			client.GetLogger().DebugContext(ctx, "WaitForTransformReady", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)

	// TODO: retry if error status.
	// if _, retryErr := client.RetryDataTransform(ctx, name); retryErr != nil {
	// 	return nil, fmt.Errorf("retrying transform: %w", retryErr)
	// }
}

// WaitForTransformDeleted polls until a data transform returns 404.
func WaitForTransformDeleted(ctx context.Context, client *sfapi.Client, name string) error {
	_, err := backoff.Retry(
		ctx,
		func() (_ struct{}, err error) {
			var sfErr *sfapi.SalesforceError
			_, err = client.GetDataTransform(ctx, name)
			if errors.As(err, &sfErr) && sfErr.IsNotFound() {
				err = nil
			} else {
				err = errors.Join(fmt.Errorf("waiting for transform deleted"), err)
			}
			return
		},
		backoff.WithNotify(func(err error, dur time.Duration) {
			client.GetLogger().DebugContext(ctx, "WaitForTransformDeleted", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)

	return err
}

// WaitForTransformRun polls the run status of a data transform until it reaches a terminal state.
func WaitForTransformRun(ctx context.Context, client *sfapi.Client, name string) (*sftypes.DataTransform, error) {
	return backoff.Retry(
		ctx,
		func() (*sftypes.DataTransform, error) {
			if _, err := client.RefreshDataTransformStatus(ctx, name); err != nil {
				return nil, fmt.Errorf("refreshing transform status: %w", err)
			}

			dt, err := client.GetDataTransform(ctx, name)
			if err != nil {
				return nil, backoff.Permanent(fmt.Errorf("getting transform run status: %w", err))
			}

			if dt.LastRunStatus.IsTerminal() {
				return dt, nil
			}

			return dt, fmt.Errorf("transform status is %v", dt.LastRunStatus)
		},
		backoff.WithNotify(func(err error, dur time.Duration) {
			client.GetLogger().DebugContext(ctx, "WaitForTransformRun", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)
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
	return backoff.Retry(
		ctx,
		func() (*sftypes.DataLakeObject, error) {
			dlo, err := client.GetDataLakeObject(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("waiting for DLO active: %v", err)
			}

			switch {
			case strings.EqualFold(string(dlo.Status), "Processing"):
				return dlo, fmt.Errorf("dlo still processing")
			case strings.EqualFold(string(dlo.Status), "Deleting"):
				return dlo, backoff.Permanent(fmt.Errorf("DLO is being deleted"))
			case strings.EqualFold(string(dlo.Status), "Active"):
				return dlo, nil
			case strings.EqualFold(string(dlo.Status), "Error"):
				return dlo, nil
			case strings.EqualFold(string(dlo.Status), "Inactive"):
				return dlo, nil // TODO: unsure about this status
			default:
				return nil, backoff.Permanent(fmt.Errorf("something went wrong. unknown DLO status %q", dlo.Status))
			}
		},
		backoff.WithNotify(func(err error, dur time.Duration) {
			client.GetLogger().DebugContext(ctx, "WaitForDLOActive", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)
}

// WaitForDLODeleted polls until a data lake object returns 404.
func WaitForDLODeleted(ctx context.Context, client *sfapi.Client, name string) error {
	_, err := backoff.Retry(
		ctx,
		func() (_ struct{}, err error) {
			var sfErr *sfapi.SalesforceError
			_, err = client.GetDataLakeObject(ctx, name)
			if errors.As(err, &sfErr) && sfErr.IsNotFound() {
				err = nil
			} else {
				err = errors.Join(fmt.Errorf("waiting for DLO deleted"), err)
			}
			return
		},
		backoff.WithNotify(func(err error, dur time.Duration) {
			client.GetLogger().DebugContext(ctx, "WaitForDLODeleted", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)

	return err
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
	// dt, err := WaitForTransformReady(ctx, client, name)
	// if err != nil {
	// 	return fmt.Errorf("waiting for transform to stabilize before delete: %w", err)
	// }
	// dt, err := client.GetDataTransform(ctx, name)
	// if err != nil {
	// 	var sfErr *sfapi.SalesforceError
	// 	if errors.As(err, &sfErr) && sfErr.IsNotFound() {
	// 		return nil
	// 	}
	// 	return fmt.Errorf("getting transform before delete: %w", err)
	// }
	// if dt.Status.IsProcessing() {

	// }

	err := client.DeleteDataTransform(ctx, name)
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

	odos, ok := validation.OutputDataObjects[req.Name]
	if !ok {
		return nil, fmt.Errorf("validated outputDataObjects malformed: expected %q, found %v",
			req.Name,
			slices.Collect(maps.Keys(validation.OutputDataObjects)),
		)
	}

	for i := range odos {
		// Set defaults the validation endpoint doesn't provide but the create endpoint requires.
		if odos[i].Label == "" {
			odos[i].Label = odos[i].Name
		}
		if odos[i].Category == "" {
			odos[i].Category = "Profile"
		}
		for j := range odos[i].Fields {
			if odos[i].Fields[j].Name == primaryKeyFieldName {
				odos[i].Fields[j].IsPrimaryKey = true
			}
			if odos[i].Fields[j].Label == "" {
				odos[i].Fields[j].Label = odos[i].Fields[j].Name
			}
		}
	}

	req.Definition.OutputDataObjects = odos

	return client.CreateDataTransform(ctx, req)
}
