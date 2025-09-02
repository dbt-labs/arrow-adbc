package main

import (
	"context"
	"fmt"
	"log"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/api"
	shared "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/examples/shared"
)

func main() {
	fmt.Println("Salesforce Data Cloud - Data Transform Examples")
	fmt.Println("===============================================")

	fmt.Println("\n=== JWT Authentication ===")
	client, err := shared.DemonstrateJWTAuth()
	if err != nil {
		log.Fatalf("JWT Auth failed: %v", err)
		return
	}

	fmt.Println("\n=== Data Transform API Example ===")
	demonstrateDCSQLDataTransform(client)
}

func demonstrateDCSQLDataTransform(client *api.Client) error {
	ctx := context.Background()

	// Create a more complex DCSQL transform with multiple nodes and dependencies
	nodes := map[string]api.DbtDataTransformNode{
		"nodeName2": api.NewSimpleDbtDataTransformNode(
			"nodeName2",
			"customers_stg__dll",
			"SELECT \"CustomerId__c\" FROM \"customers_raw__dll\"",
		),
	}

	// Create the DCSQL transform request
	request := api.NewBatchDataTransformRequest(
		"demonstrateDCSQLDataTransform",
		"demonstrateDCSQLDataTransform Example",
		nodes,
	)

	fmt.Printf("Creating DCSQL transform: %s\n", request.Name)
	fmt.Printf("   Label: %s\n", request.Label)
	fmt.Printf("   Type: %s\n", request.Type)
	fmt.Printf("   Definition Type: %s\n", request.Definition.Type)
	fmt.Printf("   Nodes: %d\n", len(request.Definition.Manifest.Nodes))

	// Execute the request
	response, err := client.CreateDataTransform(ctx, request)
	if err != nil {
		fmt.Printf("ERROR: DCSQL transform creation failed: %v\n", err)
		return err
	}

	// Display results
	fmt.Printf("✅ Advanced DCSQL transform created successfully!\n")
	fmt.Printf("   Transform ID: %s\n", response.ID)
	fmt.Printf("   Name: %s\n", response.Name)
	fmt.Printf("   Label: %s\n", response.Label)
	fmt.Printf("   Status: %s\n", response.Status)
	fmt.Printf("   Type: %s\n", response.Type)
	fmt.Printf("   Created Date: %s\n", response.CreatedDate)
	fmt.Printf("   Created By: %s\n", response.CreatedBy.Name)
	fmt.Printf("   Last Run Status: %s\n", response.LastRunStatus)
	fmt.Printf("   URL: %s\n", response.URL)

	// Display node information with dependencies
	if len(response.Definition.Manifest.Nodes) > 0 {
		fmt.Printf("   Transform Nodes: %d\n", len(response.Definition.Manifest.Nodes))
		for nodeName, node := range response.Definition.Manifest.Nodes {
			fmt.Printf("     - %s: %s\n", nodeName, node.Name)
			if node.RelationName != "" {
				fmt.Printf("       Target: %s\n", node.RelationName)
			}
			if node.Config.Materialized != "" {
				fmt.Printf("       Materialized: %s\n", node.Config.Materialized)
			}
			if len(node.DependsOn) > 0 {
				fmt.Printf("       Dependencies: %v\n", node.DependsOn)
			}
		}
	}

	return nil
}
