package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gempages/go-helper/errors"
	"github.com/gempages/go-helper/tracing"
	"github.com/gempages/go-shopify-graphql/utils"
	"github.com/getsentry/sentry-go"
	"golang.org/x/net/context/ctxhttp"
)

const MaxCostExceeded = "MAX_COST_EXCEEDED"

// Client is a GraphQL client.
type Client struct {
	url        string // GraphQL server URL.
	httpClient *http.Client
	ctx        context.Context
	retries    int
}

// NewClient creates a GraphQL client targeting the specified GraphQL server URL.
// If httpClient is nil, then http.DefaultClient is used.
func NewClient(url string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		url:        url,
		httpClient: httpClient,
	}
}

// SetContext set a context for graphql client
// set input ctx for graphql client
func (c *Client) SetContext(ctx context.Context) {
	c.ctx = ctx
}

// SetRetries set a context for graphql client
// set input ctx for graphql client
func (c *Client) SetRetries(retries int) {
	c.retries = retries
}

// Context get a single context from graphql client
// response the context from graphql client or new context
func (c *Client) Context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}

// QueryString executes a single GraphQL query request,
// using the given raw query `q` and populating the response into the `v`.
// `q` should be a correct GraphQL request string that corresponds to the GraphQL schema.
func (c *Client) QueryString(ctx context.Context, q string, variables map[string]interface{}, v interface{}) error {
	return c.do(ctx, q, variables, v)
}

// Query executes a single GraphQL query request,
// with a query derived from q, populating the response into it.
// q should be a pointer to struct that corresponds to the GraphQL schema.
func (c *Client) Query(ctx context.Context, q interface{}, variables map[string]interface{}) error {
	query := constructQuery(q, variables)
	return c.do(ctx, query, variables, q)
}

// Mutate executes a single GraphQL mutation request,
// with a mutation derived from m, populating the response into it.
// m should be a pointer to struct that corresponds to the GraphQL schema.
func (c *Client) Mutate(ctx context.Context, m interface{}, variables map[string]interface{}) error {
	query := constructMutation(m, variables)
	// return nil
	return c.do(ctx, query, variables, m)
}

// MutateString executes a single GraphQL mutation request,
// using the given raw query `m` and populating the response into it.
// `m` should be a correct GraphQL mutation request string that corresponds to the GraphQL schema.
func (c *Client) MutateString(ctx context.Context, m string, variables map[string]interface{}, v interface{}) error {
	return c.do(ctx, m, variables, v)
}

// do executes a single GraphQL operation.
func (c *Client) do(ctx context.Context, query string, variables map[string]interface{}, v interface{}) error {
	if c.ctx != nil {
		ctx = c.ctx
	}
	var err error
	in := struct {
		Query     string                 `json:"query"`
		Variables map[string]interface{} `json:"variables,omitempty"`
	}{
		Query:     query,
		Variables: variables,
	}

	// sentry tracing
	span := sentry.StartSpan(ctx, "shopify_graphql.send")
	span.Description = utils.GetDescriptionFromQuery(query)
	span.Data = map[string]interface{}{
		"GraphQL Query":     query,
		"GraphQL Variables": variables,
		"URL":               c.url,
	}
	defer func() {
		tracing.FinishSpan(span, err)
	}()
	// end sentry tracing

	retries := c.retries
	attempts := 0
	for {
		attempts++
		// Create new data buffer for each attempt
		var buf bytes.Buffer
		err = json.NewEncoder(&buf).Encode(in)
		if err != nil {
			return err
		}
		err = c.doRequest(ctx, &buf, v)
		if err == nil {
			break
		}
		if retries <= 1 {
			return fmt.Errorf("after %v attempts: %w", attempts, err)
		}
		if c.shouldRetry(err) {
			retries--
			time.Sleep(time.Duration(attempts) * time.Second)
			continue
		}
		return err
	}
	return nil
}

func (c *Client) doRequest(ctx context.Context, body io.Reader, v interface{}) error {
	resp, err := ctxhttp.Post(ctx, c.httpClient, c.url, "application/json", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPaymentRequired {
		return ErrPaymentRequired
	}
	if resp.StatusCode == http.StatusLocked {
		return ErrLocked
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return ErrUnauthorized
	}
	if resp.StatusCode == http.StatusForbidden {
		return ErrForbidden
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode == http.StatusInternalServerError {
		return ErrInternal
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.NewErrorWithContext(ctx, fmt.Errorf("non-200 OK status code: %v", resp.Status), map[string]any{"body": string(body)})
	}
	var out struct {
		Data   *json.RawMessage
		Errors graphErrors
		// Extensions interface{} // Unused.
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	if err != nil {
		// TODO: Consider including response body in returned error, if deemed helpful.
		return err
	}
	// xx := make(map[string]interface{})
	if out.Data != nil {
		err := json.Unmarshal(*out.Data, v)
		if err != nil {
			// TODO: Consider including response body in returned error, if deemed helpful.
			return err
		}
	}
	if len(out.Errors) > 0 {
		for _, e := range out.Errors {
			if e.Extensions.Code == MaxCostExceeded {
				return ErrMaxCostExceeded
			}
		}
		return out.Errors
	}
	return nil
}

func (c *Client) shouldRetry(err error) bool {
	if uerr, isURLErr := err.(*url.Error); isURLErr {
		return uerr.Timeout() || uerr.Temporary()
	}
	return isThrottledError(err) || isConnectionError(err) || errors.Is(err, ErrMaxCostExceeded)
}

// errors represents the "errors" array in a response from a GraphQL server.
// If returned via error interface, the slice is expected to contain at least 1 element.
//
// Specification: https://facebook.github.io/graphql/#sec-Errors.
type graphErrors []struct {
	Message    string
	Extensions struct {
		Code          string
		Cost          int
		MaxCost       int `json:"maxCost"`
		Documentation string
	}
	Locations []struct {
		Line   int
		Column int
	}
}

// Error implements error interface.
func (e graphErrors) Error() string {
	return e[0].Message
}

type operationType uint8

const (
	queryOperation operationType = iota
	mutationOperation
	// subscriptionOperation // Unused.
)

func isThrottledError(err error) bool {
	return err != nil && err.Error() == "Throttled"
}

func isConnectionError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "connection reset by peer") || strings.Contains(err.Error(), "broken pipe"))
}
