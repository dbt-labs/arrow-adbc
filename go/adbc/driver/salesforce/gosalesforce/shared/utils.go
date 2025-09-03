package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/api"
)

func GetEnvOrPanic(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required. Please set it before running this example.", key)
	}
	return value
}

func DemonstrateJWTAuth() (*api.Client, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home directory: %w", err)
	}

	privateKeyPath := fmt.Sprintf("%s/salesforce/JWT/server.key", home)
	if _, err := os.Stat(privateKeyPath); os.IsNotExist(err) {
		fmt.Printf("WARNING: Private key file not found at: %s\n", privateKeyPath)
		fmt.Println("   Please ensure the private key file exists or update the path")
		return nil, nil
	}

	privateKey, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key file: %w", err)
	}

	config, err := api.NewJWTConfig(
		"https://login.salesforce.com",
		GetEnvOrPanic("SALESFORCE_CLIENT_ID"),
		"storm.050b6314da1346@salesforce.com",
		string(privateKey),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT config: %w", err)
	}

	// Create client
	client := api.NewClient(config, "v64.0")

	// Authenticate
	fmt.Println("Connecting to Salesforce CDP with JWT...")
	err = client.Authenticate(context.Background())
	if err != nil {
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	fmt.Println("Connection successful!")
	PrettyPrintJSON(client.GetToken())

	fmt.Println("\nTesting CDP token exchange...")
	err = client.ExchangeAndSetDataCloudToken(context.Background())
	if err != nil {
		fmt.Printf("WARNING: CDP token exchange failed: %v\n", err)
		fmt.Println("   This might be expected if CDP is not enabled for your org")
		return client, nil
	} else {
		fmt.Println("CDP token exchange successful!")
		fmt.Printf("CDP Instance URL: %s\n", client.GetDataCloudToken().InstanceURL)
		return client, nil
	}
}

func PrettyPrintJSON[T any](v T) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Printf("Failed to marshal object: %v\n", err)
		return
	}
	fmt.Println(string(b))
}

func DeleteIfDloExists(ctx context.Context, client *api.Client, name string) error {
	fmt.Printf("Checking if DLO exists: %s\n", name)

	// Delete all data transforms that are targeting the DLO
	dataTransforms, err := client.GetDataTransformByDLO(ctx, name)
	PrettyPrintJSON(dataTransforms)
	if err != nil {
		return fmt.Errorf("failed to get data transform by DLO: %w", err)
	}

	for _, dataTransform := range dataTransforms {
		err = DeleteDataTransformIfExists(ctx, client, dataTransform.Name)
		if err != nil {
			return fmt.Errorf("failed to delete data transform: %w", err)
		}
	}

	// Delete the DLO
	deletionInProgress := false
	for {
		existingDLO, err := client.GetDataLakeObjectByName(ctx, name)
		if err == nil {
			if !deletionInProgress {
				fmt.Printf("🚨 DLO already exists (ID: %s), deleting it first...\n", existingDLO.ID)
				err = client.DeleteDataLakeObjectByName(ctx, name)
				if err != nil {
					return fmt.Errorf("failed to delete existing DLO: %w", err)
				} else {
					deletionInProgress = true
				}
			}

			// Wait for deletion to complete and verify
			fmt.Println("🕒 Waiting 5 seconds for deletion to complete...")
			time.Sleep(5 * time.Second)
		} else {
			fmt.Printf("✅ DLO does not exist, proceeding with creation\n")
			return nil
		}
	}
}

func DeleteDataTransformIfExists(ctx context.Context, client *api.Client, name string) error {
	fmt.Printf("Checking if Data Transform exists: %s\n", name)
	deletionInProgress := false
	for {
		existingDataTransform, err := client.GetDataTransform(ctx, name)
		if err == nil {
			if !deletionInProgress {
				fmt.Printf("🚨 Data Transform exists (ID: %s), deleting it first...\n", existingDataTransform.ID)
				err = client.DeleteDataTransform(ctx, name)
				if err != nil {
					return fmt.Errorf("failed to delete existing Data Transform: %w", err)
				} else {
					deletionInProgress = true
				}
			}

			fmt.Println("🕒 Waiting 5 seconds for deletion to complete...")
			time.Sleep(5 * time.Second)
		} else {
			fmt.Printf("✅ Data Transform does not exist, proceeding with creation\n")
			return nil
		}
	}
}
