package gosalesforce3

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"github.com/stretchr/testify/suite"
)

type TransformSuite struct {
	APISuite
}

func (s *TransformSuite) TestValidateDataTransform() {
	ctx := context.Background()

	// Get a real source entity
	meta, err := s.Client.GetMetadata(ctx, &types.MetadataRequest{})
	s.Require().NoError(err)
	s.Require().NotEmpty(meta.Metadata, "need at least one entity as source")
	sourceEntity := meta.Metadata[0].Name

	targetName := "sftest_validate_output__dll"

	req := &types.CreateDataTransformRequest{
		Name:  "sftest_validate",
		Label: "sftest Validate",
		Type:  types.DataTransformTypeBatch,
		Definition: types.DataTransformDefinition{
			Type:    types.DataTransformDefinitionTypeDCSQL,
			Version: "1.0",
			Manifest: types.DataTransformManifest{
				Nodes: map[string]types.DataTransformNode{
					targetName: {
						Name:         targetName,
						RelationName: targetName,
						Config: types.DataTransformNodeConfig{
							Materialized: "table",
						},
						CompiledCode: "SELECT * FROM \"" + sourceEntity + "\" LIMIT 1",
					},
				},
			},
		},
	}

	result, err := s.Client.ValidateDataTransform(ctx, req)
	s.Require().NoError(err, "validate transform failed")
	s.T().Logf("Validation: %d issues, %d output object keys", len(result.Issues), len(result.OutputDataObjects))
	for _, issue := range result.Issues {
		s.T().Logf("  [%s] %s: %s", issue.ErrorSeverity, issue.ErrorCode, issue.ErrorMessage)
	}

	// Validate should return output data objects for the target
	if odos, ok := result.OutputDataObjects[targetName]; ok {
		s.T().Logf("OutputDataObjects[%s]: %d objects", targetName, len(odos))
		for _, odo := range odos {
			s.T().Logf("  - %s (type=%s, category=%s)", odo.Name, odo.Type, odo.Category)
		}
	}
}

func TestTransformSuite(t *testing.T) {
	suite.Run(t, new(TransformSuite))
}
