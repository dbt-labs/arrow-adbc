package types

// DataStream represents a Data Cloud Data Stream.
type DataStream struct {
	ID       string `json:"id,omitempty"`
	Name     string `json:"name"`
	Label    string `json:"label,omitempty"`
	Category string `json:"category,omitempty"`
}
