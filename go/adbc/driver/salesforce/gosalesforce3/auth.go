package gosalesforce3

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/oauth2/jwt"
)

func (c *Client) Authenticate(ctx context.Context) error {
	privateKeyPEM := c.config.PrivateKeyPEM
	if !strings.Contains(privateKeyPEM, "BEGIN") {
		return fmt.Errorf("invalid private key: must be PEM-encoded")
	}

	tokenURL := strings.TrimRight(c.config.LoginURL, "/") + "/services/oauth2/token"

	jwtConf := &jwt.Config{
		Email:      c.config.ClientID,
		Subject:    c.config.Username,
		PrivateKey: []byte(privateKeyPEM),
		TokenURL:   tokenURL,
		Audience:   c.config.LoginURL,
	}

	token, err := jwtConf.TokenSource(ctx).Token()
	if err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}

	instanceURL, ok := token.Extra("instance_url").(string)
	if !ok || instanceURL == "" {
		return fmt.Errorf("authentication succeeded but instance_url not found in response")
	}

	c.instanceURL = instanceURL
	c.http.SetAuthToken(token.AccessToken)
	c.tokenSource = jwtConf.TokenSource(ctx)

	c.logger.DebugContext(ctx, "authenticated", "instance_url", instanceURL)
	return nil
}

func (c *Client) ensureAuth() error {
	if c.tokenSource == nil {
		return fmt.Errorf("client not authenticated: call Authenticate first")
	}

	token, err := c.tokenSource.Token()
	if err != nil {
		return fmt.Errorf("token refresh failed: %w", err)
	}

	c.http.SetAuthToken(token.AccessToken)
	return nil
}
