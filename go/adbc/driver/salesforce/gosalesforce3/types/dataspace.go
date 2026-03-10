package types

// DataSpaceMember represents a member of a data space.
type DataSpaceMember struct {
	Name   string        `json:"memberName"`
	Filter *FilterConfig `json:"filter,omitempty"`
}

// FilterConfig represents a filter configuration for data space members.
type FilterConfig struct {
	Expression string `json:"expression,omitempty"`
}
