package api

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cenkalti/backoff/v5"
)

// DeleteIfDloExists deletes a DLO if it exists
func (client *Client) DeleteIfDloExists(ctx context.Context, name string) error {
	// Delete all data transforms that are targeting the DLO
	dataTransforms, err := client.GetDataTransformByDLO(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to get data transform by DLO: %w", err)
	}

	for _, dataTransform := range dataTransforms {
		err = client.DeleteDataTransformIfExists(ctx, dataTransform.Name)
		if err != nil {
			return fmt.Errorf("failed to delete data transform: %w", err)
		}
	}

	// Delete the DLO using exponential backoff
	deletionTriggered := false

	// Configure exponential backoff
	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.InitialInterval = 100 * time.Millisecond
	exponentialBackOff.MaxInterval = 1 * time.Second

	operation := func() (interface{}, error) {
		_, err := client.GetDataLakeObjectByName(ctx, name)
		if err != nil {
			return nil, nil
		}

		// DLO still exists
		if !deletionTriggered {
			deleteErr := client.DeleteDataLakeObjectByName(ctx, name)
			if deleteErr != nil {
				return nil, backoff.Permanent(fmt.Errorf("failed to delete existing DLO: %w", deleteErr))
			}
			deletionTriggered = true
		}

		// Return retriable error to continue polling
		return nil, fmt.Errorf("DLO %s still exists, waiting for deletion to complete", name)
	}

	// Retry with exponential backoff
	_, err = backoff.Retry(ctx, operation,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(5*time.Minute),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 DLO deletion in progress, retrying in %v...\n", duration)
		}))

	if err != nil {
		return fmt.Errorf("timeout waiting for DLO deletion: %w", err)
	}

	return nil
}

// DeleteDataTransformIfExists deletes a Data Transform if it exists
func (client *Client) DeleteDataTransformIfExists(ctx context.Context, name string) error {
	deletionTriggered := false

	// Configure exponential backoff
	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.InitialInterval = 100 * time.Millisecond
	exponentialBackOff.MaxInterval = 1 * time.Second

	operation := func() (interface{}, error) {
		_, err := client.GetDataTransform(ctx, name)
		if err != nil {
			// Data Transform doesn't exist, deletion complete or not needed
			return nil, nil
		}

		// Data Transform still exists
		if !deletionTriggered {
			deleteErr := client.DeleteDataTransform(ctx, name)
			if deleteErr != nil {
				return nil, backoff.Permanent(fmt.Errorf("failed to delete existing Data Transform: %w", deleteErr))
			}
			deletionTriggered = true
		}

		// Return retriable error to continue polling
		return nil, fmt.Errorf("data Transform %s still exists, waiting for deletion to complete", name)
	}

	// Retry with exponential backoff
	_, err := backoff.Retry(ctx, operation,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(5*time.Minute),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 Data Transform deletion in progress, retrying in %v...\n", duration)
		}))

	if err != nil {
		return fmt.Errorf("timeout waiting for Data Transform deletion: %w", err)
	}

	return nil
}
