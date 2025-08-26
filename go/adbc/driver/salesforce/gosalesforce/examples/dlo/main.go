package main

import (
	"context"
	"fmt"
	"log"
	"time"

	shared "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/examples/shared"
	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/pkg"
)

func main() {
	fmt.Println("Salesforce Data Cloud - Data Lake Object Examples")
	fmt.Println("================================================")

	// JWT Authentication
	fmt.Println("\n=== JWT Authentication ===")
	client, err := shared.DemonstrateJWTAuth()
	if err != nil {
		log.Fatalf("JWT Auth failed: %v", err)
		return
	}

	// Data Lake Object API
	fmt.Println("\n=== Data Lake Object API Example ===")
	demonstrateDataLakeObject(client)
}

// demonstrateDataLakeObject shows how to create Data Lake Objects in Data Cloud
func demonstrateDataLakeObject(client *api.Client) {
	fmt.Println("Demonstrating Data Lake Object creation...")

	ctx := context.Background()

	// Verify tokens are available
	accessToken := client.GetToken()
	if accessToken == nil || accessToken.AccessToken == "" {
		fmt.Println("WARNING: No access token available")
		return
	}

	fmt.Printf("Using Salesforce instance: %s\n", accessToken.InstanceURL)

	// Generate timestamp for unique names
	timestamp := time.Now().Format("20060102150405")

	// Example 1: Create a Profile Data Lake Object
	fmt.Println("\n--- Creating Profile Data Lake Object ---")
	err := demonstrateProfileDLO(ctx, client, timestamp)
	if err != nil {
		fmt.Printf("ERROR: Profile DLO creation failed: %v\n", err)
	} else {
		fmt.Println("✅ Profile DLO creation successful!")
	}

	// Example 2: Create an Engagement Data Lake Object
	fmt.Println("\n--- Creating Engagement Data Lake Object ---")
	err = demonstrateEngagementDLO(ctx, client, timestamp)
	if err != nil {
		fmt.Printf("ERROR: Engagement DLO creation failed: %v\n", err)
	} else {
		fmt.Println("✅ Engagement DLO creation successful!")
	}

	// Example 3: Create a DLO with filters (data spaces)
	fmt.Println("\n--- Creating DLO with Data Space Filters ---")
	err = demonstrateDLOWithFilters(ctx, client, timestamp)
	if err != nil {
		fmt.Printf("ERROR: DLO with filters creation failed: %v\n", err)
	} else {
		fmt.Println("✅ DLO with filters creation successful!")
	}

	fmt.Println("\n✅ Data Lake Object demonstration completed!")
	fmt.Println("📊 Summary:")
	fmt.Println("   - Demonstrated Profile DLO creation")
	fmt.Println("   - Demonstrated Engagement DLO creation")
	fmt.Println("   - Demonstrated DLO with data space filters")
	fmt.Println("   - DLOs can be monitored and managed via Data Cloud UI")
}

func demonstrateProfileDLO(ctx context.Context, client *api.Client, timestamp string) error {
	// Create fields for a customer profile DLO
	fields := []api.DataLakeFieldInputRepresentation{
		api.NewDataLakeField("CustomerId", "Customer ID", api.DataLakeFieldDataTypeText, true),
		api.NewDataLakeField("FirstName", "First Name", api.DataLakeFieldDataTypeText, false),
		api.NewDataLakeField("LastName", "Last Name", api.DataLakeFieldDataTypeText, false),
		api.NewDataLakeField("Email", "Email Address", api.DataLakeFieldDataTypeText, false),
		api.NewDataLakeField("SignupDate", "Signup Date", api.DataLakeFieldDataTypeDateTime, false),
		api.NewDataLakeField("IsActive", "Is Active", api.DataLakeFieldDataTypeBoolean, false),
		api.NewDataLakeField("TotalSpent", "Total Spent", api.DataLakeFieldDataTypeNumber, false),
	}

	// Create unique name with timestamp
	name := fmt.Sprintf("CustomerProfile_Example_%s", timestamp)

	// Create the Profile DLO request using convenience function
	request := api.NewProfileDataLakeObject(name, "Customer Profile Example", fields)

	// Add optional configuration
	request.OrgUnitIdentifierFieldName = ""
	request.RecordModifiedFieldName = ""

	fmt.Printf("Creating Profile DLO: %s\n", request.Name)

	// Execute the request
	response, err := client.PostDataLakeObject(ctx, request)
	if err != nil {
		return err
	}

	// Display results
	fmt.Printf("✅ Profile DLO created successfully!\n")
	fmt.Printf("   ID: %s, Status: %s, Fields: %d\n", response.ID, response.Status, len(response.Fields))

	return nil
}

func demonstrateEngagementDLO(ctx context.Context, client *api.Client, timestamp string) error {
	// Create fields for an engagement/event DLO
	fields := []api.DataLakeFieldInputRepresentation{
		api.NewDataLakeField("EventId", "Event ID", api.DataLakeFieldDataTypeText, true),
		api.NewDataLakeField("CustomerId", "Customer ID", api.DataLakeFieldDataTypeText, false),
		api.NewDataLakeField("EventType", "Event Type", api.DataLakeFieldDataTypeText, false),
		api.NewDataLakeField("EventTimestamp", "Event Timestamp", api.DataLakeFieldDataTypeDateTime, false),
		api.NewDataLakeField("EventValue", "Event Value", api.DataLakeFieldDataTypeNumber, false),
		api.NewDataLakeField("Channel", "Channel", api.DataLakeFieldDataTypeText, false),
	}

	// Create unique name with timestamp
	name := fmt.Sprintf("CustomerEvents_Example_%s", timestamp)

	// Create the Engagement DLO request using convenience function
	request := api.NewEngagementDataLakeObject(
		name,
		"Customer Events Example",
		"EventTimestamp", // This is required for Engagement category
		fields,
	)

	// Add optional configuration
	request.OrgUnitIdentifierFieldName = ""
	request.RecordModifiedFieldName = ""

	fmt.Printf("Creating Engagement DLO: %s\n", request.Name)

	// Execute the request
	response, err := client.PostDataLakeObject(ctx, request)
	if err != nil {
		return err
	}

	// Display results
	fmt.Printf("✅ Engagement DLO created successfully!\n")
	fmt.Printf("   ID: %s, Status: %s, Fields: %d\n", response.ID, response.Status, len(response.Fields))

	return nil
}

func demonstrateDLOWithFilters(ctx context.Context, client *api.Client, timestamp string) error {
	// Create fields for a filtered DLO
	fields := []api.DataLakeFieldInputRepresentation{
		api.NewDataLakeField("OrderId", "Order ID", api.DataLakeFieldDataTypeText, true),
		api.NewDataLakeField("CustomerId", "Customer ID", api.DataLakeFieldDataTypeText, false),
		api.NewDataLakeField("Region", "Region", api.DataLakeFieldDataTypeText, false),
		api.NewDataLakeField("OrderAmount", "Order Amount", api.DataLakeFieldDataTypeNumber, false),
		api.NewDataLakeField("OrderDate", "Order Date", api.DataLakeFieldDataTypeDateTime, false),
		api.NewDataLakeField("CustomerTier", "Customer Tier", api.DataLakeFieldDataTypeText, false),
	}

	// Create unique name with timestamp
	name := fmt.Sprintf("RegionalOrders_Example_%s", timestamp)
	tableName := fmt.Sprintf("%s__dll", name)

	// Create filter conditions for US region only
	conditions := []api.FilterCondition{
		api.NewFilterCondition("Region__c", "US", tableName, api.FilterOperatorEquals),
	}

	// Create dataspace info with filters using default data space
	dataspaceInfo := []api.DataspaceInfo{
		api.NewDataspaceInfo("default", conditions, api.ConjunctiveOperatorAnd),
	}

	// Create the DLO request with basic function
	request := api.NewDataLakeObjectRequest(name, "Regional Orders Example", api.DataLakeObjectCategoryProfile, fields)

	// Add dataspace filters and other configuration
	request.DataspaceInfo = dataspaceInfo
	request.OrgUnitIdentifierFieldName = ""
	request.RecordModifiedFieldName = ""

	fmt.Printf("Creating DLO with filters: %s\n", request.Name)

	// Execute the request
	response, err := client.PostDataLakeObject(ctx, request)
	if err != nil {
		return err
	}

	// Display results
	fmt.Printf("✅ DLO with filters created successfully!\n")
	fmt.Printf("   ID: %s, Status: %s, Fields: %d, Data Spaces: %d\n",
		response.ID, response.Status, len(response.Fields), len(response.DataSpaceInfo))

	return nil
}
