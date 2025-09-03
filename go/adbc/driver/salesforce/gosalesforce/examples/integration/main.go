package main

import (
	"context"
	"fmt"
	"log"
	"time"

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

	// Infer the target DLO schema using the SQL Query API
	queryRequest := &api.SqlQueryRequest{
		SQL:      query,
		RowLimit: 0,
		// Dataspace:    "default",        // Not supported by original API
		// WorkloadName: "demonstrateSqlQuery", // Not supported by original API
	}
	sqlResponse, err := client.ExecuteSqlQuery(ctx, queryRequest)
	if err != nil {
		fmt.Printf("ERROR: Failed to execute SQL query: %v\n", err)
		return
	}
	shared.PrettyPrintJSON(sqlResponse)
	fmt.Println("✅ SQL Query executed successfully")

	// Create a DLO using fields returned from sqlResponse
	fmt.Println("\n=== Creating DLO using the inferred schema ===")
	dataLakeObject, err := CreateDataLakeObject(ctx, client, sqlResponse, targetDLO, "CustomerId_child__c", api.DataLakeObjectCategoryProfile)
	if err != nil {
		fmt.Printf("ERROR: Failed to create DLO from SQL response: %v\n", err)
		return
	}
	shared.PrettyPrintJSON(dataLakeObject)
	fmt.Println("✅ DLO created using the inferred schema")

	dataTransform, err := createDCSQLDataTransform(ctx, client, dataLakeObject, query)
	if err != nil {
		fmt.Printf("ERROR: Failed to create DCSQL data transform: %v\n", err)
		return
	}
	shared.PrettyPrintJSON(dataTransform)
	fmt.Println("✅ DCSQL data transform succeeded!")
}

// CreateDataLakeObject creates a Data Lake Object using the metadata from a SQL query response
func CreateDataLakeObject(ctx context.Context, client *api.Client, sqlResponse *api.SqlQueryResponse, targetDLOName string, primaryKeyFieldName string, category api.DataLakeObjectCategory) (*api.DataLakeObject, error) {
	// Infer DLO request from SQL response metadata
	request := api.NewDataLakeObjectFromSqlResponse(
		targetDLOName,
		targetDLOName,
		category,
		sqlResponse,
		primaryKeyFieldName,
	)

	// Set required fields to empty as they're optional
	request.OrgUnitIdentifierFieldName = ""
	request.RecordModifiedFieldName = ""

	// Create the DLO
	fmt.Println("\n=== Creating DLO Request ===")
	shared.PrettyPrintJSON(request)
	dataLakeObject, err := client.PostDataLakeObject(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to create DLO: %w", err)
	}
	return dataLakeObject, nil
}

func createDCSQLDataTransform(ctx context.Context, client *api.Client, dataLakeObject *api.DataLakeObject, sql string) (*api.DataTransform, error) {
	err := client.DeleteDataTransformIfExists(ctx, dataLakeObject.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to delete Data Transform: %w", err)
	}

	// Creates a data transform
	request := api.NewBatchDataTransformRequest(
		dataLakeObject.Name,
		fmt.Sprintf("Create the target DLO %s", dataLakeObject.Name),
		map[string]api.DbtDataTransformNode{
			"node": api.NewSimpleDbtDataTransformNode(
				"node",
				dataLakeObject.Name,
				sql,
			),
		},
	)
	dataTransform, err := client.CreateDataTransform(ctx, request)
	if err != nil {
		return nil, err
	}

	// Waits for the data transform to be active
	for {
		// noticed that RefreshDataTransformStatus always returns a non-success response
		// when invoked immediately after the data transform is created
		time.Sleep(1 * time.Second)

		// Eagerly refreshes status, otherwise `client.GetDataTransform` may respond with a stale status
		refreshStatusResponse, err := client.RefreshDataTransformStatus(ctx, dataTransform.Name)
		if err != nil {
			return nil, err
		}
		if !refreshStatusResponse.Success {
			// on warns on the non-success response
			fmt.Printf("WARNING: DCSQL transform status refresh failed: %v\n", refreshStatusResponse.Errors)
		}

		dataTransform, err := client.GetDataTransform(ctx, dataTransform.Name)
		if err != nil {
			return nil, err
		}
		if dataTransform.IsActive() {
			break
		} else if dataTransform.IsError() {
			return nil, err
		}
	}

	// Runs the data transform
	runResponse, err := client.RunDataTransform(ctx, dataTransform.Name)
	if err != nil {
		return nil, err
	}
	if !runResponse.Success {
		return nil, err
	}

	// Waits for the data transform to be active
	for {
		time.Sleep(5 * time.Second)
		refreshStatusResponse, err := client.RefreshDataTransformStatus(ctx, dataTransform.Name)
		if err != nil {
			return nil, err
		}

		dataTransform, err := client.GetDataTransform(ctx, dataTransform.Name)
		if err != nil {
			return nil, err
		}

		if dataTransform.IsLastRunSuccess() {
			break
		} else if dataTransform.IsLastRunFailure() || dataTransform.IsLastRunCanceled() {
			fmt.Printf("ERROR: DCSQL transform last run did not complete successfully: %v\n", refreshStatusResponse.Errors)
			return nil, err
		}
		if !refreshStatusResponse.Success {
			fmt.Printf("WARNING: DCSQL transform status refresh failed: %v\n", refreshStatusResponse.Errors)
		}
	}

	return dataTransform, nil
}
