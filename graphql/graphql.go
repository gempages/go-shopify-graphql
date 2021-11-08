package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"time"

	"golang.org/x/net/context/ctxhttp"
)

// Client is a GraphQL client.
type Client struct {
	url        string // GraphQL server URL.
	httpClient *http.Client
}

type Extensions struct {
	Cost *Cost `json:"cost"`
}

type Cost struct {
	RequestedQueryCost float64 `json:"requestedQueryCost"`
	ActualQueryCost    float64 `json:"actualQueryCost"`
	ThrottleStatus     struct {
		MaximumAvailable   float64 `json:"maximumAvailable"`
		CurrentlyAvailable float64 `json:"currentlyAvailable"`
		RestoreRate        float64 `json:"restoreRate"`
	} `json:"throttleStatus"`
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
	fmt.Println(query)
	// return nil
	return c.do(ctx, query, variables, m)
}

// do executes a single GraphQL operation.
func (c *Client) do(ctx context.Context, query string, variables map[string]interface{}, v interface{}) error {
	in := struct {
		Query     string                 `json:"query"`
		Variables map[string]interface{} `json:"variables,omitempty"`
	}{
		Query:     query,
		Variables: variables,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(in)
	if err != nil {
		return err
	}
	resp, err := ctxhttp.Post(ctx, c.httpClient, c.url, "application/json", &buf)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("non-200 OK status code: %v body: %q", resp.Status, body)
	}
	var out struct {
		Data       *json.RawMessage
		Errors     errors
		Extensions *Extensions `json:"extensions"` // Unused.
	}
	err = json.NewDecoder(resp.Body).Decode(&out)

	if len(out.Errors) > 0 && out.Extensions != nil {
		if out.Errors[0].Message == "Throttled" {
			if out.Extensions.Cost != nil {
				requestedQueryCost := out.Extensions.Cost.RequestedQueryCost
				throttleStatus := out.Extensions.Cost.ThrottleStatus
				currentlyAvailable := throttleStatus.CurrentlyAvailable
				restoreRate := throttleStatus.RestoreRate
				if currentlyAvailable < requestedQueryCost {
					timeSleep := math.Ceil((requestedQueryCost - currentlyAvailable) / restoreRate)
					time.Sleep(time.Duration(timeSleep) * time.Second)
				}
			}
		}
	}

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
		return out.Errors
	}
	return nil
}

// errors represents the "errors" array in a response from a GraphQL server.
// If returned via error interface, the slice is expected to contain at least 1 element.
//
// Specification: https://facebook.github.io/graphql/#sec-Errors.
type errors []struct {
	Message   string
	Locations []struct {
		Line   int
		Column int
	}
}

// Error implements error interface.
func (e errors) Error() string {
	return e[0].Message
}

type operationType uint8

const (
	queryOperation operationType = iota
	mutationOperation
	//subscriptionOperation // Unused.
)
