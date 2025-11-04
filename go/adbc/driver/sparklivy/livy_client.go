// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sparklivy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// LivyClient handles communication with the Livy REST API
type LivyClient struct {
	baseURL    string
	httpClient *http.Client
	authType   string
	awsConfig  aws.Config
	username   string
	password   string
}

// NewLivyClient creates a new Livy client
func NewLivyClient(baseURL string, httpClient *http.Client, authType string, awsConfig aws.Config, username, password string) *LivyClient {
	return &LivyClient{
		baseURL:    baseURL,
		httpClient: httpClient,
		authType:   authType,
		awsConfig:  awsConfig,
		username:   username,
		password:   password,
	}
}

// Session represents a Livy session
type Session struct {
	ID                  int                    `json:"id"`
	AppID               string                 `json:"appId"`
	Owner               string                 `json:"owner"`
	ProxyUser           string                 `json:"proxyUser"`
	Kind                string                 `json:"kind"`
	Log                 []string               `json:"log"`
	State               string                 `json:"state"`
	AppInfo             map[string]interface{} `json:"appInfo"`
	HeartbeatTimeoutSec int                    `json:"heartbeatTimeoutInSecond,omitempty"`
	TTL                 string                 `json:"ttl,omitempty"`
}

// SessionState represents possible session states
type SessionState string

const (
	SessionStateNotStarted   SessionState = "not_started"
	SessionStateStarting     SessionState = "starting"
	SessionStateIdle         SessionState = "idle"
	SessionStateBusy         SessionState = "busy"
	SessionStateShuttingDown SessionState = "shutting_down"
	SessionStateError        SessionState = "error"
	SessionStateDead         SessionState = "dead"
	SessionStateKilled       SessionState = "killed"
	SessionStateSuccess      SessionState = "success"
)

// Statement represents a Livy statement
type Statement struct {
	ID        int              `json:"id"`
	Code      string           `json:"code"`
	State     string           `json:"state"`
	Output    *StatementOutput `json:"output"`
	Progress  float64          `json:"progress"`
	Started   int64            `json:"started"`
	Completed int64            `json:"completed"`
}

// StatementOutput represents statement output
type StatementOutput struct {
	Status         string                 `json:"status"`
	ExecutionCount int                    `json:"execution_count"`
	Data           map[string]interface{} `json:"data"`
	Ename          string                 `json:"ename"`
	Evalue         string                 `json:"evalue"`
	Traceback      []string               `json:"traceback"`
}

// StatementState represents possible statement states
type StatementState string

const (
	StatementStateWaiting    StatementState = "waiting"
	StatementStateRunning    StatementState = "running"
	StatementStateAvailable  StatementState = "available"
	StatementStateError      StatementState = "error"
	StatementStateCancelling StatementState = "cancelling"
	StatementStateCancelled  StatementState = "cancelled"
)

// CreateSessionRequest represents a session creation request
type CreateSessionRequest struct {
	Kind                string            `json:"kind"`
	ProxyUser           string            `json:"proxyUser,omitempty"`
	Jars                []string          `json:"jars,omitempty"`
	PyFiles             []string          `json:"pyFiles,omitempty"`
	Files               []string          `json:"files,omitempty"`
	DriverMemory        string            `json:"driverMemory,omitempty"`
	DriverCores         int               `json:"driverCores,omitempty"`
	ExecutorMemory      string            `json:"executorMemory,omitempty"`
	ExecutorCores       int               `json:"executorCores,omitempty"`
	NumExecutors        int               `json:"numExecutors,omitempty"`
	Archives            []string          `json:"archives,omitempty"`
	Queue               string            `json:"queue,omitempty"`
	Name                string            `json:"name,omitempty"`
	Conf                map[string]string `json:"conf,omitempty"`
	HeartbeatTimeoutSec int               `json:"heartbeatTimeoutInSecond,omitempty"`
	TTL                 string            `json:"ttl,omitempty"`
}

// CreateStatementRequest represents a statement execution request
type CreateStatementRequest struct {
	Code string `json:"code"`
	Kind string `json:"kind,omitempty"`
}

// CreateSession creates a new Livy session
func (c *LivyClient) CreateSession(ctx context.Context, req CreateSessionRequest) (*Session, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal session request: %w", err)
	}

	resp, err := c.doRequest(ctx, "POST", "/sessions", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to create session: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var session Session
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		return nil, fmt.Errorf("failed to decode session response: %w", err)
	}

	return &session, nil
}

// GetSession retrieves session information
func (c *LivyClient) GetSession(ctx context.Context, sessionID int) (*Session, error) {
	url := fmt.Sprintf("/sessions/%d", sessionID)
	resp, err := c.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get session: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var session Session
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		return nil, fmt.Errorf("failed to decode session response: %w", err)
	}

	return &session, nil
}

// DeleteSession deletes a session
func (c *LivyClient) DeleteSession(ctx context.Context, sessionID int) error {
	url := fmt.Sprintf("/sessions/%d", sessionID)
	resp, err := c.doRequest(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete session: status=%d, body=%s", resp.StatusCode, string(body))
	}

	return nil
}

// WaitForSessionReady waits for the session to be in idle state
func (c *LivyClient) WaitForSessionReady(ctx context.Context, sessionID int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for session to be ready")
			}

			session, err := c.GetSession(ctx, sessionID)
			if err != nil {
				return fmt.Errorf("failed to get session status: %w", err)
			}

			switch SessionState(session.State) {
			case SessionStateIdle:
				return nil
			case SessionStateError, SessionStateDead, SessionStateKilled:
				return fmt.Errorf("session failed with state: %s", session.State)
			case SessionStateStarting, SessionStateNotStarted:
				// Continue waiting
				continue
			default:
				return fmt.Errorf("unexpected session state: %s", session.State)
			}
		}
	}
}

// CreateStatement executes a statement in a session
func (c *LivyClient) CreateStatement(ctx context.Context, sessionID int, req CreateStatementRequest) (*Statement, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal statement request: %w", err)
	}

	url := fmt.Sprintf("/sessions/%d/statements", sessionID)
	resp, err := c.doRequest(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to create statement: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var stmt Statement
	if err := json.NewDecoder(resp.Body).Decode(&stmt); err != nil {
		return nil, fmt.Errorf("failed to decode statement response: %w", err)
	}

	return &stmt, nil
}

// GetStatement retrieves statement information
func (c *LivyClient) GetStatement(ctx context.Context, sessionID, statementID int) (*Statement, error) {
	url := fmt.Sprintf("/sessions/%d/statements/%d", sessionID, statementID)
	resp, err := c.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get statement: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var stmt Statement
	if err := json.NewDecoder(resp.Body).Decode(&stmt); err != nil {
		return nil, fmt.Errorf("failed to decode statement response: %w", err)
	}

	return &stmt, nil
}

// WaitForStatementComplete waits for a statement to complete
func (c *LivyClient) WaitForStatementComplete(ctx context.Context, sessionID, statementID int, timeout time.Duration) (*Statement, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("timeout waiting for statement to complete")
			}

			stmt, err := c.GetStatement(ctx, sessionID, statementID)
			if err != nil {
				return nil, fmt.Errorf("failed to get statement status: %w", err)
			}

			switch StatementState(stmt.State) {
			case StatementStateAvailable, StatementStateError:
				return stmt, nil
			case StatementStateCancelled:
				return nil, fmt.Errorf("statement was cancelled")
			case StatementStateWaiting, StatementStateRunning:
				// Continue waiting
				continue
			default:
				return nil, fmt.Errorf("unexpected statement state: %s", stmt.State)
			}
		}
	}
}

// doRequest performs an HTTP request with appropriate authentication
func (c *LivyClient) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	url := c.baseURL + path

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Apply authentication
	switch c.authType {
	case AuthTypeAWSSigV4:
		if err := c.signRequestWithSigV4(ctx, req); err != nil {
			return nil, fmt.Errorf("failed to sign request: %w", err)
		}
	case AuthTypeBasic:
		req.SetBasicAuth(c.username, c.password)
	case AuthTypeNone:
		// No authentication
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	return resp, nil
}

// signRequestWithSigV4 signs an HTTP request using AWS SigV4
func (c *LivyClient) signRequestWithSigV4(ctx context.Context, req *http.Request) error {
	// Get credentials
	creds, err := c.awsConfig.Credentials.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}

	// Create signer
	signer := v4.NewSigner()

	// Read body if present (for signing)
	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return fmt.Errorf("failed to read request body: %w", err)
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	// Compute payload hash
	hash := sha256.Sum256(bodyBytes)
	payloadHash := hex.EncodeToString(hash[:])

	// Sign the request
	// Service name for EMR Serverless Livy is "emr-serverless"
	err = signer.SignHTTP(ctx, creds, req, payloadHash, "emr-serverless", c.awsConfig.Region, time.Now())
	if err != nil {
		return fmt.Errorf("failed to sign request with SigV4: %w", err)
	}

	// Restore body
	if bodyBytes != nil {
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	return nil
}
