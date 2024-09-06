package clients

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-logr/logr"
)

// StackRoxClientBuilder contains the data and logic needed to create a client for the StackRox API. Don't create
// instances of this type directly, use the NewStackRoxClient function instead.
type StackRoxClientBuilder struct {
	logger logr.Logger
	url    string
	token  string
	ca     *x509.CertPool
}

// StackRoxClient simplifies the usage with the StackRox API. Don't create instances of this type directly, use the
// NewStackRoxClient function instead.
type StackRoxClient struct {
	logger logr.Logger
	url    string
	token  string
	client *http.Client
}

// NewStackRoxClient creates a client that simplifies the usage of the StackRox API.
func NewStackroxClient() *StackRoxClientBuilder {
	return &StackRoxClientBuilder{}
}

// SetLogger sets the logger that the client will use to write to the log. This is mandatory.
func (b *StackRoxClientBuilder) SetLogger(value logr.Logger) *StackRoxClientBuilder {
	b.logger = value
	return b
}

// SetURL sets the base URL of the StackRox API server. This is mandatory.
func (b *StackRoxClientBuilder) SetURL(value string) *StackRoxClientBuilder {
	b.url = value
	return b
}

// SetToken sets the token that will be used to authenticate to the StackRox API server. This mandatory.
func (b *StackRoxClientBuilder) SetToken(value string) *StackRoxClientBuilder {
	b.token = value
	return b
}

// SetCA sets the CA certificates that will be trusted to connect to the StackRox API server. This is optional. By
// default all the system CA certificates will be trusted.
func (b *StackRoxClientBuilder) SetCA(value *x509.CertPool) *StackRoxClientBuilder {
	b.ca = value
	return b
}

// Build uses the configuration stored in the builder to create a new API client.
func (b *StackRoxClientBuilder) Build() (result *StackRoxClient, err error) {
	// Check parameters:
	if b.logger.GetSink() == nil {
		err = errors.New("logger is mandatory")
		return
	}
	if b.url == "" {
		err = errors.New("base URL is mandatory")
		return
	}
	if b.token == "" {
		err = errors.New("token is mandatory")
		return
	}

	// Create the HTTP client:
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: b.ca,
		},
	}
	client := &http.Client{
		Transport: transport,
	}

	// Create and populate the object:
	result = &StackRoxClient{
		logger: b.logger,
		url:    b.url,
		token:  b.token,
		client: client,
	}
	return
}

func (s *StackRoxClient) DoRequest(method, path string, body string) ([]byte, *int, error) {
	endpoint := fmt.Sprintf("%s%s", s.url, path)
	req, err := http.NewRequest(method, endpoint, strings.NewReader(body))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request (method: %s, path: %s, body: %s): %v", method, path, body, err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", s.token))
	req.Header.Add("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to make request (method: %s, path: %s, body: %s): %v", method, path, body, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response (method: %s, path: %s, body: %s): %v", method, path, body, err)
	}

	return respBody, &resp.StatusCode, nil
}
