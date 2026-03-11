package types

// DataCloudActionResponse is the standard response for action endpoints.
type DataCloudActionResponse struct {
	Success bool     `json:"success,string"`
	Errors  []string `json:"errors,omitempty"`
}

// DataObjectCategory is the category of a data object (DLO, DMO, OutputDataObject, etc.).
type DataObjectCategory = string

const (
	CategoryProfile    DataObjectCategory = "Profile"
	CategoryEngagement DataObjectCategory = "Engagement"
	CategoryOther      DataObjectCategory = "Other"
)

// Example usage:
//
//	type Dummy struct {
//		// ...
//	}
//
//	type DummyCollection struct {
//		Paginated                     // embedded without json tag
//		Items[Dummy] `json:"dummies"` // embedded with json tag
//	}
type Pagination interface {
	// TODO: add helper methods working with paginated collections
	// The idea is that this interface is automatically satisfied by any struct embedding [Paginated] and [Items] (e.g. DummyCollection in the example above).
	// This allows us to write helper functions that take in a Pagination and work with the common pagination fields, without needing to know the specific type of items in the collection.
	//
	// The methods here should be a union of the [Paginated] and [Items] methods.
}

// Paginated is meant to be embedded in a `...Collection` struct.
//
// These are the common fields for all paginated responses for Data 360 Connect.
// Should be used in combination with [Items] to create Collection types for each resource.
//
// See [Pagination] for usage.
type Paginated struct {
	// TotalSize of the collection.
	TotalSize int64 `json:"totalSize"`

	// CurrentPageURL.
	CurrentPageUrl string `json:"currentPageUrl"`

	// NextPageURL, if it exists.
	NextPageUrl string `json:"nextPageUrl"`
}

// Items is meant to be embedded in a `...Collection` struct with a `json` tag.
//
// Data 360 Connect doesn't use a common key for the list of resources.
// Should be used in combination with [Paginated] to create Collection types for each resource.
//
// See [Pagination] for usage.
type Items[T any] []T

// TODO: create remaining collection types

type DataTransformCollection struct {
	Paginated
	Items[DataTransform] `json:"dataTransforms"`
}
