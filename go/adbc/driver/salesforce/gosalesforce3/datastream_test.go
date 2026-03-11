package gosalesforce3

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"github.com/stretchr/testify/suite"
)

type DataStreamSuite struct {
	APISuite
}

func (s *DataStreamSuite) TestGetDataStream() {
	// First get a valid data stream name from metadata
	meta, err := s.Client.GetMetadata(context.Background(), &types.MetadataRequest{})
	s.Require().NoError(err)
	s.Require().NotEmpty(meta.Metadata, "need at least one entity")

	// Use the first entity name — data streams often share names with DLOs
	ds, err := s.Client.GetDataStream(context.Background(), meta.Metadata[0].Name)
	if err != nil {
		// Data stream might not exist for this entity, that's OK
		sfErr, ok := err.(*SalesforceError)
		if ok && sfErr.IsNotFound() {
			s.T().Skipf("no data stream found for %s, skipping", meta.Metadata[0].Name)
		}
		s.Require().NoError(err)
	}
	s.NotEmpty(ds.Name)
}

func TestDataStreamSuite(t *testing.T) {
	suite.Run(t, new(DataStreamSuite))
}
