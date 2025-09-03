package main

import (
	"context"
	"fmt"
	"log"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/api"
	shared "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/shared"
)

func main() {
	fmt.Println("Salesforce Data Cloud - Data Transform Examples")
	fmt.Println("===============================================")

	// auth
	client, err := shared.DemonstrateJWTAuth()
	if err != nil {
		log.Fatalf("JWT Auth failed: %v", err)
		return
	}
	ctx := context.Background()

	// setups
	targetDLO := "customers_child__dll"
	query := "SELECT \"CustomerId__c\" as \"CustomerId_child__c\" FROM \"customers_raw__dll\""

	err = client.DeleteIfDloExists(ctx, targetDLO)
	if err != nil {
		fmt.Printf("ERROR: Failed to delete DLO: %v\n", err)
		return
	}

	// Creates a DLO using the inferred schema
	fmt.Println("\n=== Creating DLO using the inferred schema ===")
	dataLakeObject, err := client.CreateDataLakeObjectWithInferredSchema(ctx, query, targetDLO, "CustomerId_child__c", api.DataLakeObjectCategoryProfile)
	if err != nil {
		fmt.Printf("ERROR: Failed to create DLO from SQL response: %v\n", err)
		return
	} else {
		shared.PrettyPrintJSON(dataLakeObject)
		fmt.Println("✅ DLO created using the inferred schema")
	}
	shared.PrettyPrintJSON(dataLakeObject)
	fmt.Println("✅ DLO created using the inferred schema")

	// Creates a DCSQL data transform
	fmt.Println("\n=== Creating and running a dbt batch data transform ===")
	dataTransform, err := client.CreateDbtBatchDataTransform(ctx, dataLakeObject, query, true)
	if err != nil {
		fmt.Printf("ERROR: Failed to create DCSQL data transform: %v\n", err)
		return
	}
	shared.PrettyPrintJSON(dataTransform)
	fmt.Println("✅ dbt batch data transform succeeded!")
}
