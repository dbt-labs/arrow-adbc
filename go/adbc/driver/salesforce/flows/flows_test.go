package flows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sfapi "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce"
	sftypes "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/cassette"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/recorder"
	"resty.dev/v3"
)

type FlowsSuite struct {
	suite.Suite
	Client   *sfapi.Client
	recorder *recorder.Recorder
}

func TestFlows(t *testing.T) {
	if !hasRealCredentials() {
		t.Skip("skipping flow tests: no Salesforce credentials")
	}
	suite.Run(t, new(FlowsSuite))
}

func hasRealCredentials() bool {
	return os.Getenv("SFDC_LOGIN_URL") != "" &&
		os.Getenv("SFDC_CLIENT_ID") != "" &&
		os.Getenv("SFDC_USERNAME") != "" &&
		os.Getenv("SFDC_CLIENT_PRIVATE_KEY_PATH") != ""
}

func realAuthConfig(t require.TestingT) *sftypes.AuthConfig {
	keyPath := os.Getenv("SFDC_CLIENT_PRIVATE_KEY_PATH")
	if !filepath.IsAbs(keyPath) {
		keyPath = filepath.Join("..", keyPath)
	}
	keyPEM, err := os.ReadFile(keyPath)
	require.NoError(t, err)

	return &sftypes.AuthConfig{
		LoginURL:      os.Getenv("SFDC_LOGIN_URL"),
		ClientID:      os.Getenv("SFDC_CLIENT_ID"),
		Username:      os.Getenv("SFDC_USERNAME"),
		PrivateKeyPEM: string(keyPEM),
	}
}

func parseURL(raw string) *url.URL {
	u, _ := url.Parse(raw)
	return u
}

var (
	hookStripAuthHeader = recorder.WithHook(
		func(i *cassette.Interaction) error {
			delete(i.Request.Headers, "Authorization")
			return nil
		},
		recorder.BeforeSaveHook,
	)

	hookPrettyJsonBody = recorder.WithHook(
		func(i *cassette.Interaction) error {
			var (
				t   string
				err error
				buf bytes.Buffer
			)

			t, _, err = mime.ParseMediaType(i.Request.Headers.Get("Content-Type"))
			if err == nil && t == "application/json" {
				buf.Reset()
				err = json.Indent(&buf, []byte(i.Request.Body), "", "  ")
				if err == nil {
					i.Request.Body = buf.String()
				}
			}

			t, _, err = mime.ParseMediaType(i.Response.Headers.Get("Content-Type"))
			if err == nil && t == "application/json" {
				buf.Reset()
				err = json.Indent(&buf, []byte(i.Response.Body), "", "  ")
				if err == nil {
					i.Response.Body = buf.String()
				}
			}

			return nil
		},
		recorder.BeforeSaveHook,
	)
)

func (s *FlowsSuite) SetupTest() {
	t := s.T()
	cassetteName := filepath.Join("testdata", strings.ReplaceAll(t.Name(), "/", "_"))

	mode := recorder.ModeRecordOnly

	r, err := recorder.New(cassetteName,
		recorder.WithMode(mode),
		recorder.WithSkipRequestLatency(true),
		hookStripAuthHeader,
		hookPrettyJsonBody,
		recorder.WithMatcher(func(r *http.Request, i cassette.Request) bool {
			if r.Method != i.Method {
				return false
			}
			cassetteURL := parseURL(i.URL)
			return r.URL.Path == cassetteURL.Path && r.URL.RawQuery == cassetteURL.RawQuery
		}),
	)
	require.NoError(t, err)
	s.recorder = r

	withVCR := sfapi.WithModifyClient(func(c *resty.Client) {
		c.SetTransport(r)
		c.SetHeader("Accept-Encoding", "identity")
	})

	if hasRealCredentials() {
		cfg := realAuthConfig(t)
		client, err := sfapi.NewClient(cfg, withVCR)
		require.NoError(t, err)

		err = client.Authenticate(context.Background())
		require.NoError(t, err)

		s.Client = client
	} else {
		client, err := sfapi.NewClient(
			&sftypes.AuthConfig{
				LoginURL:   "https://test.salesforce.com",
				ClientID:   "test-client-id",
				Username:   "test@example.com",
				APIVersion: "v64.0",
			},
			withVCR,
		)
		require.NoError(t, err)

		client.SetBaseURL("https://test.salesforce.com")
		client.SetAuthToken("test-token")

		s.Client = client
	}
}

func (s *FlowsSuite) TearDownTest() {
	if s.recorder != nil {
		err := s.recorder.Stop()
		s.Require().NoError(err)
	}
	if s.Client != nil {
		s.Client.Close()
	}
}

func buildTransformRequest(transformName, outputDLOName, sqlQuery string) *sftypes.DataTransformRequest {
	return &sftypes.DataTransformRequest{
		Name:          transformName,
		Label:         transformName,
		Type:          sftypes.DataTransformTypeBatch,
		DataSpaceName: "default",
		Definition: sftypes.DataTransformDefinition{
			Type:    sftypes.DataTransformDefinitionTypeDCSQL,
			Version: "1.0",
			Manifest: sftypes.DataTransformManifest{
				Nodes: sftypes.DataTransformNodes{
					sftypes.DataTransformNodeID("node_" + outputDLOName): {
						Name:         outputDLOName,
						RelationName: outputDLOName,
						Config: sftypes.DataTransformNodeConfig{
							Materialized: sftypes.MaterializationTable,
							WriteMode:    sftypes.WriteModeOverwrite,
						},
						CompiledCode: sqlQuery,
					},
				},
			},
		},
	}
}

// cleanupResources deletes a transform and DLO, waiting for both to be fully gone.
// Used at the start of each test to remove leftovers from prior failed runs.
func (s *FlowsSuite) cleanupResources(ctx context.Context, transformName, dloName string) {
	t := s.T()
	if err := DeleteTransformAndWait(ctx, s.Client, transformName); err != nil {
		t.Logf("cleanup: failed to delete transform %q: %v", transformName, err)
	}
	if err := DeleteDLOAndWait(ctx, s.Client, dloName); err != nil {
		t.Logf("cleanup: failed to delete DLO %q: %v", dloName, err)
	}
}

func (s *FlowsSuite) TestCreateRunDeleteTransform() {
	ctx := context.Background()
	transformName := "flow_test_create_run_transform"
	outputDLO := "flow_test_create_run__dll"
	sql := "SELECT primary_key__c, fieldtype_Text__c FROM test_all_field_types__dll"

	s.cleanupResources(ctx, transformName, outputDLO)

	req := buildTransformRequest(transformName, outputDLO, sql)

	// Create via validate + create
	dt, err := ValidateAndCreateTransform(ctx, s.Client, req, "primary_key__c")
	s.Require().NoError(err)
	s.Require().NotNil(dt)

	// Wait for Active
	dt, err = WaitForTransformReady(ctx, s.Client, transformName)
	s.Require().NoError(err)
	s.Require().True(dt.Status.IsActive())

	// Run and wait
	dt, err = RunAndWaitForTransform(ctx, s.Client, transformName)
	s.Require().NoError(err)
	s.Require().True(dt.LastRunStatus.IsSuccess())

	// Cleanup
	err = DeleteTransformAndWait(ctx, s.Client, transformName)
	s.Require().NoError(err)

	err = DeleteDLOAndWait(ctx, s.Client, outputDLO)
	s.Require().NoError(err)
}

func (s *FlowsSuite) TestValidateAndCreateWithAutoCreateDLO() {
	ctx := context.Background()
	transformName := "flow_test_validate_create_transform"
	outputDLO := "flow_test_validate_create__dll"
	sql := "SELECT primary_key__c, fieldtype_Email__c FROM test_all_field_types__dll"

	s.cleanupResources(ctx, transformName, outputDLO)

	req := buildTransformRequest(transformName, outputDLO, sql)

	// Validate and create
	dt, err := ValidateAndCreateTransform(ctx, s.Client, req, "primary_key__c")
	s.Require().NoError(err)
	s.Require().NotNil(dt)

	// Wait for Active
	dt, err = WaitForTransformReady(ctx, s.Client, transformName)
	s.Require().NoError(err)
	s.Require().True(dt.Status.IsActive())

	// Run and wait
	dt, err = RunAndWaitForTransform(ctx, s.Client, transformName)
	s.Require().NoError(err)
	s.Require().True(dt.LastRunStatus.IsSuccess())

	// Cleanup
	err = DeleteTransformAndWait(ctx, s.Client, transformName)
	s.Require().NoError(err)

	err = DeleteDLOAndWait(ctx, s.Client, outputDLO)
	s.Require().NoError(err)
}

func (s *FlowsSuite) TestDeleteTransformAndWait() {
	ctx := context.Background()
	transformName := "flow_test_delete_transform"
	outputDLO := "flow_test_delete__dll"
	sql := "SELECT primary_key__c FROM test_all_field_types__dll"

	s.cleanupResources(ctx, transformName, outputDLO)

	req := buildTransformRequest(transformName, outputDLO, sql)

	// Create transform
	dt, err := ValidateAndCreateTransform(ctx, s.Client, req, "primary_key__c")
	s.Require().NoError(err)
	s.Require().NotNil(dt)

	// Wait for Active
	_, err = WaitForTransformReady(ctx, s.Client, transformName)
	s.Require().NoError(err)

	// Delete and wait
	err = DeleteTransformAndWait(ctx, s.Client, transformName)
	s.Require().NoError(err)

	// Verify 404
	_, err = s.Client.GetDataTransform(ctx, transformName)
	s.Require().Error(err)
	var sfErr *sfapi.SalesforceError
	s.Require().True(errors.As(err, &sfErr))
	s.Require().True(sfErr.IsNotFound())

	// Cleanup output DLO
	err = DeleteDLOAndWait(ctx, s.Client, outputDLO)
	s.Require().NoError(err)
}
