package shopify

import (
	"os"

	graphqlclient "github.com/gempages/go-shopify-graphql/graph"
	"github.com/gempages/go-shopify-graphql/graphql"

	log "github.com/sirupsen/logrus"
)

const (
	shopifyAPIVersion           = "2022-07"
	shopifyStoreFrontAPIVersion = "2022-07"
)

type Client struct {
	gql *graphql.Client

	Product       ProductService
	Variant       VariantService
	Inventory     InventoryService
	Collection    CollectionService
	Cart          CartService
	Billing       BillingService
	Order         OrderService
	Fulfillment   FulfillmentService
	Location      LocationService
	Metafield     MetafieldService
	BulkOperation BulkOperationService
	Webhook       WebhookService
}

type ListOptions struct {
	Query   string
	First   int
	Last    int
	After   string
	Before  string
	Reverse bool
}

func NewDefaultClient() (shopClient *Client) {
	apiKey := os.Getenv("STORE_API_KEY")
	password := os.Getenv("STORE_PASSWORD")
	storeName := os.Getenv("STORE_NAME")
	if apiKey == "" || password == "" || storeName == "" {
		log.Fatalln("Shopify app API Key and/or Password and/or Store Name not set")
	}

	shopClient = NewClient(apiKey, password, storeName)

	return
}

func NewClient(apiKey string, password string, storeName string) *Client {
	c := &Client{gql: newShopifyGraphQLClient(apiKey, password, storeName)}

	c.Product = &ProductServiceOp{client: c}
	c.Variant = &VariantServiceOp{client: c}
	c.Inventory = &InventoryServiceOp{client: c}
	c.Cart = &CartServiceOp{client: c}
	c.Billing = &BillingServiceOp{client: c}
	c.Collection = &CollectionServiceOp{client: c}
	c.Order = &OrderServiceOp{client: c}
	c.Fulfillment = &FulfillmentServiceOp{client: c}
	c.Location = &LocationServiceOp{client: c}
	c.Metafield = &MetafieldServiceOp{client: c}
	c.BulkOperation = &BulkOperationServiceOp{client: c}
	c.Webhook = &WebhookServiceOp{client: c}

	return c
}

func newShopifyGraphQLClient(apiKey string, password string, storeName string) *graphql.Client {
	opts := []graphqlclient.Option{
		graphqlclient.WithVersion(shopifyAPIVersion),
		graphqlclient.WithPrivateAppAuth(apiKey, password),
	}
	return graphqlclient.NewClient(storeName, opts...)
}

func (c *Client) GraphQLClient() *graphql.Client {
	return c.gql
}

func NewClientWithOpts(storeName string, opts ...graphqlclient.Option) *Client {
	c := &Client{gql: graphqlclient.NewClient(storeName, opts...)}

	c.Product = &ProductServiceOp{client: c}
	c.Variant = &VariantServiceOp{client: c}
	c.Inventory = &InventoryServiceOp{client: c}
	c.Cart = &CartServiceOp{client: c}
	c.Billing = &BillingServiceOp{client: c}
	c.Collection = &CollectionServiceOp{client: c}
	c.Order = &OrderServiceOp{client: c}
	c.Fulfillment = &FulfillmentServiceOp{client: c}
	c.Location = &LocationServiceOp{client: c}
	c.Metafield = &MetafieldServiceOp{client: c}
	c.BulkOperation = &BulkOperationServiceOp{client: c}
	c.Webhook = &WebhookServiceOp{client: c}

	return c
}

func NewClientWithToken(apiKey string, storeName string) *Client {
	c := &Client{gql: newShopifyGraphQLClientWithToken(apiKey, storeName)}

	c.Product = &ProductServiceOp{client: c}
	c.Variant = &VariantServiceOp{client: c}
	// c.Inventory = &InventoryServiceOp{client: c}
	c.Cart = &CartServiceOp{client: c}
	c.Billing = &BillingServiceOp{client: c}
	c.Collection = &CollectionServiceOp{client: c}
	// c.Order = &OrderServiceOp{client: c}
	// c.Fulfillment = &FulfillmentServiceOp{client: c}
	// c.Location = &LocationServiceOp{client: c}
	c.Metafield = &MetafieldServiceOp{client: c}
	c.BulkOperation = &BulkOperationServiceOp{client: c}
	c.Webhook = &WebhookServiceOp{client: c}

	return c
}

func NewClientStoreFrontWithToken(apiKey string, storeName string) *Client {
	c := &Client{gql: newShopifyStoreFrontGraphQLClientWithToken(apiKey, storeName)}
	c.Cart = &CartServiceOp{client: c}
	c.Product = &ProductServiceOp{client: c}
	c.Collection = &CollectionServiceOp{client: c}

	return c
}

func newShopifyGraphQLClientWithToken(token string, storeName string) *graphql.Client {
	opts := []graphqlclient.Option{
		graphqlclient.WithVersion(shopifyAPIVersion),
		graphqlclient.WithToken(token),
	}
	// todo no more fixed storeName
	return graphqlclient.NewClient(storeName, opts...)
}

func newShopifyStoreFrontGraphQLClientWithToken(token string, storeName string) *graphql.Client {
	opts := []graphqlclient.Option{
		graphqlclient.WithStoreFrontVersion(shopifyStoreFrontAPIVersion),
		graphqlclient.WithStoreFrontToken(token),
	}
	// todo no more fixed storeName
	return graphqlclient.NewClient(storeName, opts...)
}
