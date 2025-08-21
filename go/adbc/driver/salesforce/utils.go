package salesforce

import (
	"github.com/apache/arrow-go/v18/arrow"
)

func SalesforceTypeToArrow(sfType string) arrow.DataType {
	switch sfType {
	case "STRING", "TEXT", "VARCHAR":
		return arrow.BinaryTypes.String
	case "INTEGER", "INT":
		return arrow.PrimitiveTypes.Int64
	case "DECIMAL", "NUMERIC":
		return arrow.PrimitiveTypes.Float64
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	case "DATETIME", "TIMESTAMP":
		return arrow.FixedWidthTypes.Timestamp_us
	default:
		// Default to string for unknown types
		return arrow.BinaryTypes.String
	}
}
