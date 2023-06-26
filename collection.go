package shopify

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gempages/go-shopify-graphql/utils"

	"github.com/gempages/go-shopify-graphql/graphql"

	log "github.com/sirupsen/logrus"
)

type CollectionService interface {
	List(query string) ([]*CollectionBulkResult, error)
	ListAll() ([]*CollectionBulkResult, error)
	ListByCursor(first int, cursor string) (*CollectionsQueryResult, error)
	ListWithFields(first int, cursor string, query string, fields string) (*CollectionsQueryResult, error)

	Get(id graphql.ID) (*CollectionQueryResult, error)
	GetSingleCollection(id graphql.ID, cursor string) (*CollectionQueryResult, error)

	Create(collection *CollectionCreate) (graphql.ID, error)
	CreateBulk(collections []*CollectionCreate) error

	Update(collection *CollectionCreate) error
}

type CollectionServiceOp struct {
	client *Client
}

type Collection struct {
	CollectionBase
}

type CollectionImage struct {
	AltText graphql.String `json:"altText,omitempty"`
	ID      graphql.ID     `json:"id,omitempty"`
	Src     graphql.String `json:"src,omitempty"`
	Height  graphql.Int    `json:"height,omitempty"`
	Width   graphql.Int    `json:"width,omitempty"`
}

type CollectionBase struct {
	ID              graphql.ID      `json:"id,omitempty"`
	CreatedAt       time.Time       `json:"createdAt,omitempty"`
	UpdatedAt       time.Time       `json:"updatedAt,omitempty"`
	Handle          graphql.String  `json:"handle,omitempty"`
	Title           graphql.String  `json:"title,omitempty"`
	Description     graphql.String  `json:"description,omitempty"`
	DescriptionHTML graphql.String  `json:"descriptionHtml,omitempty"`
	ProductsCount   graphql.Int     `json:"productsCount,omitempty"`
	TemplateSuffix  graphql.String  `json:"templateSuffix,omitempty"`
	Seo             Seo             `json:"seo,omitempty"`
	Image           CollectionImage `json:"image,omitempty"`
}

type CollectionBulkResult struct {
	CollectionBase

	Products []ProductBulkResult `json:"products,omitempty"`
}

type CollectionsQueryResult struct {
	Collections struct {
		Edges []struct {
			Collection CollectionQueryResult `json:"node,omitempty"`
			Cursor     string                `json:"cursor,omitempty"`
		} `json:"edges,omitempty"`
		PageInfo PageInfo `json:"pageInfo,omitempty"`
	} `json:"collections,omitempty"`
}

type CollectionQueryResult struct {
	CollectionBase

	Products struct {
		Edges []struct {
			Product ProductBulkResult `json:"node,omitempty"`
			Cursor  string            `json:"cursor,omitempty"`
		} `json:"edges,omitempty"`
		PageInfo PageInfo `json:"pageInfo,omitempty"`
	} `json:"products,omitempty"`
}

type CollectionCreate struct {
	CollectionInput CollectionInput
}

type mutationCollectionCreate struct {
	CollectionCreateResult CollectionCreateResult `graphql:"collectionCreate(input: $input)" json:"collectionCreate"`
}

type mutationCollectionUpdate struct {
	CollectionCreateResult CollectionCreateResult `graphql:"collectionUpdate(input: $input)" json:"collectionUpdate"`
}

type CollectionInput struct {
	// The description of the collection, in HTML format.
	DescriptionHTML graphql.String `json:"descriptionHtml,omitempty"`

	// A unique human-friendly string for the collection. Automatically generated from the collection's title.
	Handle graphql.String `json:"handle,omitempty"`

	// Specifies the collection to update or create a new collection if absent.
	ID graphql.ID `json:"id,omitempty"`

	// The image associated with the collection.
	Image *ImageInput `json:"image,omitempty"`

	// The metafields to associate with this collection.
	Metafields []MetafieldInput `json:"metafields,omitempty"`

	// Initial list of collection products. Only valid with productCreate and without rules.
	Products []graphql.ID `json:"products,omitempty"`

	// Indicates whether a redirect is required after a new handle has been provided. If true, then the old handle is redirected to the new one automatically.
	RedirectNewHandle graphql.Boolean `json:"redirectNewHandle,omitempty"`

	//	The rules used to assign products to the collection.
	RuleSet *CollectionRuleSetInput `json:"ruleSet,omitempty"`

	// SEO information for the collection.
	SEO *SEOInput `json:"seo,omitempty"`

	// The order in which the collection's products are sorted.
	SortOrder *CollectionSortOrder `json:"sortOrder,omitempty"`

	// The theme template used when viewing the collection in a store.
	TemplateSuffix graphql.String `json:"templateSuffix,omitempty"`

	// Required for creating a new collection.
	Title graphql.String `json:"title,omitempty"`
}

type CollectionRuleSetInput struct {
	// Whether products must match any or all of the rules to be included in the collection. If true, then products must match one or more of the rules to be included in the collection. If false, then products must match all of the rules to be included in the collection.
	AppliedDisjunctively graphql.Boolean `json:"appliedDisjunctively"` // REQUIRED

	// The rules used to assign products to the collection.
	Rules []CollectionRuleInput `json:"rules,omitempty"`
}

type CollectionRuleInput struct {
	// The attribute that the rule focuses on (for example, title or product_type).
	Column CollectionRuleColumn `json:"column,omitempty"` // REQUIRED

	// The value that the operator is applied to (for example, Hats).
	Condition graphql.String `json:"condition,omitempty"` // REQUIRED

	// The type of operator that the rule is based on (for example, equals, contains, or not_equals).
	Relation CollectionRuleRelation `json:"relation,omitempty"` // REQUIRED
}

// CollectionRuleColumn string enum
// VENDOR The vendor attribute.
// TAG The tag attribute.
// TITLE The title attribute.
// TYPE The type attribute.
// VARIANT_COMPARE_AT_PRICE The variant_compare_at_price attribute.
// VARIANT_INVENTORY The variant_inventory attribute.
// VARIANT_PRICE The variant_price attribute.
// VARIANT_TITLE The variant_title attribute.
// VARIANT_WEIGHT The variant_weight attribute.
// IS_PRICE_REDUCED The is_price_reduced attribute.
type CollectionRuleColumn string

// CollectionRuleRelation enum
// STARTS_WITH The attribute starts with the condition.
// ENDS_WITH The attribute ends with the condition.
// EQUALS The attribute is equal to the condition.
// GREATER_THAN The attribute is greater than the condition.
// IS_NOT_SET The attribute is not set.
// IS_SET The attribute is set.
// LESS_THAN The attribute is less than the condition.
// NOT_CONTAINS The attribute does not contain the condition.
// NOT_EQUALS The attribute does not equal the condition.
// CONTAINS The attribute contains the condition.
type CollectionRuleRelation string

// CollectionSortOrder enum
// PRICE_DESC By price, in descending order (highest - lowest).
// ALPHA_DESC Alphabetically, in descending order (Z - A).
// BEST_SELLING By best-selling products.
// CREATED By date created, in ascending order (oldest - newest).
// CREATED_DESC By date created, in descending order (newest - oldest).
// MANUAL In the order set manually by the merchant.
// PRICE_ASC By price, in ascending order (lowest - highest).
// ALPHA_ASC Alphabetically, in ascending order (A - Z).
type CollectionSortOrder string

type CollectionCreateResult struct {
	Collection struct {
		ID graphql.ID `json:"id,omitempty"`
	}
	UserErrors []UserErrors
}

var collectionQuery = `
	id
	handle
	title

	products(first:250, after: $cursor){
		edges{
			node{
				id
			}
			cursor
		}
		pageInfo{
			hasNextPage
		}
	}
`

var collectionSingleQuery = `
  id
  title
  createdAt
  updatedAt
  handle
  description
  descriptionHtml
  templateSuffix
  productsCount
  seo {
    description
    title
  }
  products(first: 250) {
    edges {
      node {
        id
      }
      cursor
    }
  }
`

var collectionSingleQueryWithCursor = `
  id
  title
  handle
  createdAt
  updatedAt
  description
  descriptionHtml
  templateSuffix
  productsCount
  seo {
    description
    title
  }
  products(first: 250, after: $cursor) {
    edges {
      node {
        id
      }
      cursor
    }
  }
`

var collectionBulkQuery = `
	id
	handle
	title
    createdAt
	updatedAt
 	description
    descriptionHtml
	templateSuffix
	productsCount
	seo{
	  description
	  title
	}
	image {
		altText
		height
		id
		src
		width
	}
`

var collectionWithProductsBulkQuery = `
	id
	handle
	title
    createdAt
	updatedAt
 	description
    descriptionHtml
	templateSuffix
	productsCount
	seo{
	  description
	  title
	}
	image {
		altText
		height
		id
		src
		width
	}
	products {
		edges {
		  node {
			id
		  }
		  cursor
		}
	}
`

func (s *CollectionServiceOp) List(query string) ([]*CollectionBulkResult, error) {
	q := fmt.Sprintf(`
		{
			collections(query: "$query"){
				edges{
					node{
						%s
					}
				}
			}
		}
	`, collectionWithProductsBulkQuery)

	q = strings.ReplaceAll(q, "$query", query)

	res := []*CollectionBulkResult{}
	err := s.client.BulkOperation.BulkQuery(q, &res)
	if err != nil {
		return []*CollectionBulkResult{}, err
	}

	return res, nil
}

func (s *CollectionServiceOp) ListAll() ([]*CollectionBulkResult, error) {
	q := fmt.Sprintf(`
		{
			collections{
				edges{
					node{
						%s
					}
				}
			}
		}
	`, collectionBulkQuery)

	res := []*CollectionBulkResult{}
	err := s.client.BulkOperation.BulkQuery(q, &res)
	if err != nil {
		return []*CollectionBulkResult{}, err
	}

	return res, nil
}

func (s *CollectionServiceOp) ListByCursor(first int, cursor string) (*CollectionsQueryResult, error) {
	q := fmt.Sprintf(`
		query collections($first: Int!, $cursor: String) {
			collections(first: $first, after: $cursor){
                edges{
					node {
						%s
					}
                    cursor
                }
                pageInfo {
                      hasNextPage
                }
			}
		}
	`, collectionBulkQuery)

	vars := map[string]interface{}{
		"first": first,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}

	out := CollectionsQueryResult{}
	err := utils.ExecWithRetries(s.client.retries, func() error {
		return s.client.gql.QueryString(context.Background(), q, vars, &out)
	})
	if err != nil {
		return nil, err
	}

	return &out, nil
}

func (s *CollectionServiceOp) ListWithFields(first int, cursor, query, fields string) (*CollectionsQueryResult, error) {
	if fields == "" {
		fields = `id`
	}

	q := fmt.Sprintf(`
		query collections($first: Int!, $cursor: String, $query: String) {
			collections(first: $first, after: $cursor, query:$query){
				edges{
					cursor
					node {
						%s
					}
				}
			}
		}
	`, fields)

	vars := map[string]interface{}{
		"first": first,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}
	if query != "" {
		vars["query"] = query
	}
	out := CollectionsQueryResult{}

	err := utils.ExecWithRetries(s.client.retries, func() error {
		return s.client.gql.QueryString(context.Background(), q, vars, &out)
	})
	if err != nil {
		return nil, err
	}

	return &out, nil
}

func (s *CollectionServiceOp) Get(id graphql.ID) (*CollectionQueryResult, error) {
	var (
		out *CollectionQueryResult
		err error
	)
	out, err = s.getPage(id, "")
	if err != nil {
		return nil, err
	}

	nextPageData := out
	hasNextPage := out.Products.PageInfo.HasNextPage
	for hasNextPage && len(nextPageData.Products.Edges) > 0 {
		cursor := nextPageData.Products.Edges[len(nextPageData.Products.Edges)-1].Cursor
		// Shopify rate limit: 2 requests per sec
		time.Sleep(500 * time.Millisecond)
		nextPageData, err = s.getPage(id, cursor)
		if err != nil {
			return nil, err
		}
		out.Products.Edges = append(out.Products.Edges, nextPageData.Products.Edges...)
		hasNextPage = nextPageData.Products.PageInfo.HasNextPage
	}

	return out, nil
}

func (s *CollectionServiceOp) getPage(id graphql.ID, cursor string) (*CollectionQueryResult, error) {
	q := fmt.Sprintf(`
		query collection($id: ID!, $cursor: String) {
			collection(id: $id){
				%s
			}
		}
	`, collectionQuery)

	vars := map[string]interface{}{
		"id": id,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}

	out := struct {
		Collection *CollectionQueryResult `json:"collection"`
	}{}
	err := utils.ExecWithRetries(s.client.retries, func() error {
		return s.client.gql.QueryString(context.Background(), q, vars, &out)
	})
	if err != nil {
		return nil, err
	}

	return out.Collection, nil
}

func (s *CollectionServiceOp) GetSingleCollection(id graphql.ID, cursor string) (*CollectionQueryResult, error) {
	q := ""
	if cursor != "" {
		q = fmt.Sprintf(`
    query collection($id: ID!, $cursor: String) {
      collection(id: $id){
        %s
      }
    }
    `, collectionSingleQueryWithCursor)
	} else {
		q = fmt.Sprintf(`
    query collection($id: ID!) {
      collection(id: $id){
        %s
      }
    }
    `, collectionSingleQuery)
	}

	vars := map[string]interface{}{
		"id": id,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}

	out := struct {
		Collection *CollectionQueryResult `json:"collection"`
	}{}
	err := utils.ExecWithRetries(s.client.retries, func() error {
		return s.client.gql.QueryString(context.Background(), q, vars, &out)
	})
	if err != nil {
		return nil, err
	}

	return out.Collection, nil
}

func (s *CollectionServiceOp) CreateBulk(collections []*CollectionCreate) error {
	for _, c := range collections {
		_, err := s.client.Collection.Create(c)
		if err != nil {
			log.Warnf("Couldn't create collection (%v): %s", c, err)
		}
	}

	return nil
}

func (s *CollectionServiceOp) Create(collection *CollectionCreate) (graphql.ID, error) {
	var id graphql.ID
	m := mutationCollectionCreate{}

	vars := map[string]interface{}{
		"input": collection.CollectionInput,
	}
	err := utils.ExecWithRetries(s.client.retries, func() error {
		return s.client.gql.Mutate(context.Background(), &m, vars)
	})
	if err != nil {
		return id, err
	}

	if len(m.CollectionCreateResult.UserErrors) > 0 {
		return id, fmt.Errorf("%+v", m.CollectionCreateResult.UserErrors)
	}

	id = m.CollectionCreateResult.Collection.ID
	return id, nil
}

func (s *CollectionServiceOp) Update(collection *CollectionCreate) error {
	m := mutationCollectionUpdate{}

	vars := map[string]interface{}{
		"input": collection.CollectionInput,
	}
	err := utils.ExecWithRetries(s.client.retries, func() error {
		return s.client.gql.Mutate(context.Background(), &m, vars)
	})
	if err != nil {
		return err
	}

	if len(m.CollectionCreateResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.CollectionCreateResult.UserErrors)
	}

	return nil
}
