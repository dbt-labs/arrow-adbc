package gosalesforce3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSalesforceError_Error(t *testing.T) {
	err := &SalesforceError{StatusCode: 404, Code: "NOT_FOUND", Message: "Resource not found"}
	assert.Equal(t, "salesforce 404 NOT_FOUND: Resource not found", err.Error())
	assert.True(t, err.IsNotFound())
	assert.False(t, err.IsRateLimited())
}

func TestSalesforceError_ErrorWithoutCode(t *testing.T) {
	err := &SalesforceError{StatusCode: 500, Message: "Internal server error"}
	assert.Equal(t, "salesforce 500: Internal server error", err.Error())
}
