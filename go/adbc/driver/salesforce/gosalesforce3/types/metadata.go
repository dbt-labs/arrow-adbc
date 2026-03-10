package types

type MetadataRequest struct {
	Dataspace      string
	EntityCategory string
	EntityName     string
	EntityType     string
}

type MetadataResponse struct {
	Metadata []MetadataEntity `json:"metadata"`
}

type MetadataEntity struct {
	Name       string          `json:"name"`
	Label      string          `json:"label"`
	Category   string          `json:"category"`
	EntityType string          `json:"entityType"`
	Fields     []MetadataField `json:"fields"`
}

type MetadataField struct {
	Name       string `json:"name"`
	Label      string `json:"label"`
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primaryKey"`
}
