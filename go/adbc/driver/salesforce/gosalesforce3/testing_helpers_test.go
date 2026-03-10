package gosalesforce3

import (
	"path/filepath"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/oauth2"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/cassette"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/recorder"
	"resty.dev/v3"
)

type APISuite struct {
	suite.Suite
	Client   *Client
	recorder *recorder.Recorder
}

func (s *APISuite) SetupTest() {
	t := s.T()
	cassetteName := filepath.Join("testdata", sanitizeCassetteName(t.Name()))

	r, err := recorder.New(cassetteName,
		recorder.WithMode(recorder.ModeRecordOnce),
		recorder.WithSkipRequestLatency(true),
		recorder.WithHook(func(i *cassette.Interaction) error {
			delete(i.Request.Headers, "Authorization")
			return nil
		}, recorder.BeforeSaveHook),
		recorder.WithMatcher(cassette.NewDefaultMatcher(
			cassette.WithIgnoreAuthorization(),
		)),
	)
	require.NoError(t, err)
	s.recorder = r

	restyClient := resty.New()
	restyClient.SetTransport(r)
	restyClient.SetHeader("Content-Type", "application/json")

	client, err := NewClient(
		testAuthConfig(),
		WithHTTPClient(restyClient),
	)
	require.NoError(t, err)

	client.instanceURL = "https://test.salesforce.com"
	client.tokenSource = oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: "test-token",
	})
	client.http.SetAuthToken("test-token")

	s.Client = client
}

func (s *APISuite) TearDownTest() {
	if s.recorder != nil {
		err := s.recorder.Stop()
		s.Require().NoError(err)
	}
	if s.Client != nil {
		s.Client.Close()
	}
}

func testAuthConfig() *types.AuthConfig {
	return &types.AuthConfig{
		LoginURL:   "https://test.salesforce.com",
		ClientID:   "test-client-id",
		Username:   "test@example.com",
		APIVersion: "v64.0",
	}
}

func sanitizeCassetteName(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}
