package shopify

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gempages/go-shopify-graphql/graphql"

	log "github.com/sirupsen/logrus"
)

type ProductService interface {
	List(query string) ([]*ProductBulkResult, error)
	ListAll() ([]*ProductBulkResult, error)
	ListWithFields(first int, cursor string, query string, fields string) (*ProductsQueryResult, error)

	Get(gid graphql.ID) (*ProductQueryResult, error)
	GetSingleProductCollection(id graphql.ID, cursor string) (*ProductQueryResult, error)
	GetSingleProductVariant(id graphql.ID, cursor string) (*ProductQueryResult, error)
	GetSingleProduct(id graphql.ID) (*ProductQueryResult, error)
	Create(product *ProductCreate) error
	CreateBulk(products []*ProductCreate) error

	Update(product *ProductUpdate) error
	UpdateBulk(products []*ProductUpdate) error

	Delete(product *ProductDelete) error
	DeleteBulk(products []*ProductDelete) error
	TriggerListAll() (id graphql.ID, err error)
}

type ProductServiceOp struct {
	client *Client
}

type ProductBase struct {
	ID               graphql.ID           `json:"id,omitempty"`
	CreatedAt        time.Time            `json:"createdAt,omitempty"`
	LegacyResourceID graphql.String       `json:"legacyResourceId,omitempty"`
	Handle           graphql.String       `json:"handle,omitempty"`
	Options          []ProductOption      `json:"options,omitempty"`
	Tags             []graphql.String     `json:"tags,omitempty"`
	Description      graphql.String       `json:"description,omitempty"`
	Title            graphql.String       `json:"title,omitempty"`
	PriceRangeV2     *ProductPriceRangeV2 `json:"priceRangeV2,omitempty"`
	ProductType      graphql.String       `json:"productType,omitempty"`
	Vendor           graphql.String       `json:"vendor,omitempty"`
	TotalInventory   graphql.Int          `json:"totalInventory,omitempty"`
	OnlineStoreURL   graphql.String       `json:"onlineStoreUrl,omitempty"`
	DescriptionHTML  graphql.String       `json:"descriptionHtml,omitempty"`
	SEO              SEOInput             `json:"seo,omitempty"`
	TemplateSuffix   graphql.String       `json:"templateSuffix,omitempty"`
	Status           graphql.String       `json:"status,omitempty"`
	PublishedAt      time.Time            `json:"publishedAt,omitempty"`
	UpdatedAt        time.Time            `json:"updatedAt,omitempty"`
	TracksInventory  bool                 `json:"tracksInventory,omitempty"`
}

type ProductBulkResult struct {
	ProductBase

	Metafields      []Metafield      `json:"metafields,omitempty"`
	ProductVariants []ProductVariant `json:"variants,omitempty"`
	Collections     []Collection     `json:"collections,omitempty"`
	ProductImages   []ProductImage   `json:"images,omitempty"`
}

type ProductImage struct {
	AltText graphql.String `json:"altText,omitempty"`
	ID      graphql.ID     `json:"id,omitempty"`
	Src     graphql.String `json:"src,omitempty"`
	Height  graphql.Int    `json:"height,omitempty"`
	Width   graphql.Int    `json:"width,omitempty"`
}

// SEO information.
type Seo struct {
	// SEO Description.
	Description graphql.String `json:"description,omitempty"`
	// SEO Title.
	Title graphql.String `json:"title,omitempty"`
}

type ProductsQueryResult struct {
	Products struct {
		Edges []struct {
			Product ProductQueryResult `json:"node,omitempty"`
			Cursor  string             `json:"cursor,omitempty"`
		} `json:"edges,omitempty"`
		PageInfo PageInfo `json:"pageInfo,omitempty"`
	} `json:"products,omitempty"`
}

type ProductQueryResult struct {
	ProductBase

	ProductVariants struct {
		Edges []struct {
			Variant ProductVariant `json:"node,omitempty"`
			Cursor  string         `json:"cursor,omitempty"`
		} `json:"edges,omitempty"`
		PageInfo PageInfo `json:"pageInfo,omitempty"`
	} `json:"variants,omitempty"`
	Collections struct {
		Edges []struct {
			Collection Collection `json:"node,omitempty"`
			Cursor     string     `json:"cursor,omitempty"`
		} `json:"edges,omitempty"`
		PageInfo PageInfo `json:"pageInfo,omitempty"`
	} `json:"collections,omitempty"`
	Images struct {
		Edges []struct {
			Collection Collection `json:"node,omitempty"`
			Cursor     string     `json:"cursor,omitempty"`
		} `json:"edges,omitempty"`
		PageInfo PageInfo `json:"pageInfo,omitempty"`
	} `json:"images,omitempty"`
}

type ProductShort struct {
	ID     graphql.ID       `json:"id,omitempty"`
	Handle graphql.String   `json:"handle,omitempty"`
	Tags   []graphql.String `json:"tags,omitempty"`
}

type ProductOption struct {
	ID       graphql.ID       `json:"id,omitempty"`
	Name     graphql.String   `json:"name,omitempty"`
	Position graphql.Int      `json:"position,omitempty"`
	Values   []graphql.String `json:"values,omitempty"`
}

type ProductPriceRangeV2 struct {
	MinVariantPrice MoneyV2 `json:"minVariantPrice,omitempty"`
	MaxVariantPrice MoneyV2 `json:"maxVariantPrice,omitempty"`
}

type ProductCreate struct {
	ProductInput ProductInput
	MediaInput   []CreateMediaInput
}

type ProductUpdate struct {
	ProductInput ProductInput
}

type ProductDelete struct {
	ProductInput ProductDeleteInput
}

type ProductDeleteInput struct {
	ID graphql.ID `json:"id,omitempty"`
}

type ProductInput struct {
	// The IDs of the collections that this product will be added to.
	CollectionsToJoin []graphql.ID `json:"collectionsToJoin,omitempty"`

	// The IDs of collections that will no longer include the product.
	CollectionsToLeave []graphql.ID `json:"collectionsToLeave,omitempty"`

	// The description of the product, complete with HTML formatting.
	DescriptionHTML graphql.String `json:"descriptionHtml,omitempty"`

	// Whether the product is a gift card.
	GiftCard graphql.Boolean `json:"giftCard,omitempty"`

	// The theme template used when viewing the gift card in a store.
	GiftCardTemplateSuffix graphql.String `json:"giftCardTemplateSuffix,omitempty"`

	// A unique human-friendly string for the product. Automatically generated from the product's title.
	Handle graphql.String `json:"handle,omitempty"`

	// Specifies the product to update in productUpdate or creates a new product if absent in productCreate.
	ID graphql.ID `json:"id,omitempty"`

	// The images to associate with the product.
	Images []ImageInput `json:"images,omitempty"`

	// The metafields to associate with this product.
	Metafields []MetafieldInput `json:"metafields,omitempty"`

	// List of custom product options (maximum of 3 per product).
	Options []graphql.String `json:"options,omitempty"`

	// The product type specified by the merchant.
	ProductType graphql.String `json:"productType,omitempty"`

	// Whether a redirect is required after a new handle has been provided. If true, then the old handle is redirected to the new one automatically.
	RedirectNewHandle graphql.Boolean `json:"redirectNewHandle,omitempty"`

	// The SEO information associated with the product.
	SEO *SEOInput `json:"seo,omitempty"`

	// A comma separated list tags that have been added to the product.
	Tags []graphql.String `json:"tags,omitempty"`

	// The theme template used when viewing the product in a store.
	TemplateSuffix graphql.String `json:"templateSuffix,omitempty"`

	// The title of the product.
	Title graphql.String `json:"title,omitempty"`

	// A list of variants associated with the product.
	Variants []ProductVariantInput `json:"variants,omitempty"`

	// The name of the product's vendor.
	Vendor graphql.String `json:"vendor,omitempty"`
}

type CreateMediaInput struct {
	Alt              graphql.String   `json:"alt,omitempty"`
	MediaContentType MediaContentType `json:"mediaContentType,omitempty"` // REQUIRED
	OriginalSource   graphql.String   `json:"originalSource,omitempty"`   // REQUIRED
}

// MediaContentType enum
// EXTERNAL_VIDEO An externally hosted video.
// IMAGE A Shopify hosted image.
// MODEL_3D A 3d model.
// VIDEO A Shopify hosted video.
type MediaContentType string

type MetafieldInput struct {
	ID        graphql.ID         `json:"id,omitempty"`
	Namespace graphql.String     `json:"namespace,omitempty"`
	Key       graphql.String     `json:"key,omitempty"`
	Value     graphql.String     `json:"value,omitempty"`
	Type      MetafieldValueType `json:"type,omitempty"`
}

// MetafieldValueType enum
// INTEGER An integer.
// JSON_STRING A JSON string.
// STRING A string.
type MetafieldValueType string

type SEOInput struct {
	Description graphql.String `json:"description,omitempty"`
	Title       graphql.String `json:"title,omitempty"`
}

type ImageInput struct {
	AltText graphql.String `json:"altText,omitempty"`
	ID      graphql.ID     `json:"id,omitempty"`
	Src     graphql.String `json:"src,omitempty"`
}

type mutationProductCreate struct {
	ProductCreateResult productCreateResult `graphql:"productCreate(input: $input, media: $media)" json:"productCreate"`
}

type mutationProductUpdate struct {
	ProductUpdateResult productUpdateResult `graphql:"productUpdate(input: $input)" json:"productUpdate"`
}

type mutationProductDelete struct {
	ProductDeleteResult productDeleteResult `graphql:"productDelete(input: $input)" json:"productDelete"`
}

type productCreateResult struct {
	Product struct {
		ID graphql.ID `json:"id,omitempty"`
	}
	UserErrors []UserErrors `json:"userErrors"`
}

type productUpdateResult struct {
	Product struct {
		ID graphql.ID `json:"id,omitempty"`
	}
	UserErrors []UserErrors `json:"userErrors"`
}

type productDeleteResult struct {
	ID         string       `json:"deletedProductId,omitempty"`
	UserErrors []UserErrors `json:"userErrors"`
}

const productBaseQuery = `
  id
  legacyResourceId
  handle
  status
  publishedAt
  createdAt
  updatedAt
  tracksInventory
	options{
    	id
		name
		position
		values
	}
	tags
	title
	description
	priceRangeV2{
		minVariantPrice{
			amount
			currencyCode
		}
		maxVariantPrice{
			amount
			currencyCode
		}
	}
	productType
	vendor
	totalInventory
	onlineStoreUrl
	descriptionHtml
	seo{
		description
		title
	}
	templateSuffix
`

var singleProductQueryVariant = fmt.Sprintf(`
  id
  variants(first: 100) {
    edges {
      node {
        id
		createdAt
		updatedAt
        legacyResourceId
        sku
        selectedOptions {
          name
          value
        }
        compareAtPrice
        price
        inventoryQuantity
		image {
			altText
			height
			id
			src
			width
		}
        barcode
        title
        inventoryPolicy
        inventoryManagement
        weightUnit
        weight
      }
      cursor
    }
  }

`)

var singleProductQueryVariantWithCursor = fmt.Sprintf(`
  id
  variants(first: 100, after: $cursor) {
    edges {
      node {
        id
		createdAt
		updatedAt
        legacyResourceId
        sku
        selectedOptions {
          name
          value
        }
        compareAtPrice
        price
        inventoryQuantity
        barcode
        title
        inventoryPolicy
        inventoryManagement
        weightUnit
        weight
      }
      cursor
    }
  }

`)

var singleProductQueryCollection = fmt.Sprintf(`
  id
  collections(first:250) {
    edges {
      node {
        id
        title
        handle
        description
        templateSuffix
       	image {
			altText
			height
			id
			src
			width
		}
      }
      cursor
    }
  }
`)

var singleProductQueryCollectionWithCursor = fmt.Sprintf(`
  id
  collections(first:250, after: $cursor) {
    edges {
      node {
        id
		title
        handle
        description
        templateSuffix
		image {
			altText
			height
			id
			src
			width
		}
      }
      cursor
    }
  }
`)

var productQuery = fmt.Sprintf(`
	%s
	variants(first:100, after: $cursor){
		edges{
			node{
				id
				createdAt
				updatedAt
				legacyResourceId
				sku
				selectedOptions{
					name
					value
				}
				compareAtPrice
				price
				inventoryQuantity
				barcode
				title
				inventoryPolicy
				inventoryManagement
				weightUnit
				weight
			}
		}
	}
`, productBaseQuery)

var productQueryWithCollection = fmt.Sprintf(`
	%s
  collections {
    edges {
      node {
        id
        title
        handle
        description
        templateSuffix
        productsCount
        seo{
          description
          title
        }
      }
    }
  }
`, productQuery)

var productBulkQuery = fmt.Sprintf(`
	%s
	metafields{
		edges{
			node{
				id
				legacyResourceId
				namespace
				key
				value
				type
			}
		}
	}
  collections {
    edges {
      node {
        id
  		updatedAt
        title
        handle
        description
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
      }
    }
  }
    images {
        edges {
            node {
                altText
                height
                id
                src
                width
            }
        }
    }
	variants{
		edges{
			node{
				id
				createdAt
				updatedAt
				legacyResourceId
				sku
				selectedOptions{
					name
					value
				}
                image {
                    altText
                    height
                    id
                    src
                    width
                }
				compareAtPrice
				price
				inventoryQuantity
				barcode
				title
				inventoryPolicy
				inventoryManagement
				weightUnit
				weight
			}
		}
	}
`, productBaseQuery)

func (s *ProductServiceOp) ListAll() ([]*ProductBulkResult, error) {
	q := fmt.Sprintf(`
		{
			products{
				edges{
					node{
						%s
					}
				}
			}
		}
	`, productBulkQuery)

	res := []*ProductBulkResult{}
	err := s.client.BulkOperation.BulkQuery(q, &res)
	if err != nil {
		return []*ProductBulkResult{}, err
	}

	return res, nil
}

func (s *ProductServiceOp) TriggerListAll() (id graphql.ID, err error) {
	q := fmt.Sprintf(`
		{
			products{
				edges{
					node{
						%s
					}
				}
			}
		}
	`, productBulkQuery)

	res := []*ProductBulkResult{}
	id, err = s.client.BulkOperation.BulkQueryRunOnly(q, &res)
	return id, err
}

func (s *ProductServiceOp) List(query string) ([]*ProductBulkResult, error) {
	q := fmt.Sprintf(`
		{
			products(query: "$query"){
				edges{
					node{
						%s
					}
				}
			}
		}
	`, productBulkQuery)

	q = strings.ReplaceAll(q, "$query", query)

	res := []*ProductBulkResult{}
	err := s.client.BulkOperation.BulkQuery(q, &res)
	if err != nil {
		return []*ProductBulkResult{}, err
	}

	return res, nil
}

func (s *ProductServiceOp) Get(id graphql.ID) (*ProductQueryResult, error) {
	out, err := s.getPage(id, "")
	if err != nil {
		return nil, err
	}

	nextPageData := out
	hasNextPage := out.ProductVariants.PageInfo.HasNextPage
	for hasNextPage && len(nextPageData.ProductVariants.Edges) > 0 {
		cursor := nextPageData.ProductVariants.Edges[len(nextPageData.ProductVariants.Edges)-1].Cursor
		nextPageData, err := s.getPage(id, cursor)
		if err != nil {
			return nil, err
		}
		out.ProductVariants.Edges = append(out.ProductVariants.Edges, nextPageData.ProductVariants.Edges...)
		hasNextPage = nextPageData.ProductVariants.PageInfo.HasNextPage
	}

	return out, nil
}

func (s *ProductServiceOp) getPage(id graphql.ID, cursor string) (*ProductQueryResult, error) {
	q := fmt.Sprintf(`
		query product($id: ID!, $cursor: String) {
			product(id: $id){
				%s
			}
		}
	`, productQuery)

	vars := map[string]interface{}{
		"id": id,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}

	out := struct {
		Product *ProductQueryResult `json:"product"`
	}{}
	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out.Product, nil
}

func (s *ProductServiceOp) GetSingleProductCollection(id graphql.ID, cursor string) (*ProductQueryResult, error) {
	q := ""
	if cursor != "" {
		q = fmt.Sprintf(`
    query product($id: ID!, $cursor: String) {
      product(id: $id){
        %s
      }
    }
    `, singleProductQueryCollectionWithCursor)
	} else {
		q = fmt.Sprintf(`
    query product($id: ID!) {
      product(id: $id){
        %s
      }
    }
    `, singleProductQueryCollection)
	}

	vars := map[string]interface{}{
		"id": id,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}

	out := struct {
		Product *ProductQueryResult `json:"product"`
	}{}
	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out.Product, nil
}

func (s *ProductServiceOp) GetSingleProductVariant(id graphql.ID, cursor string) (*ProductQueryResult, error) {
	q := ""
	if cursor != "" {
		q = fmt.Sprintf(`
    query product($id: ID!, $cursor: String) {
      product(id: $id){
        %s
      }
    }
    `, singleProductQueryVariantWithCursor)
	} else {
		q = fmt.Sprintf(`
    query product($id: ID!) {
      product(id: $id){
        %s
      }
    }
    `, singleProductQueryVariant)
	}

	vars := map[string]interface{}{
		"id": id,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}

	out := struct {
		Product *ProductQueryResult `json:"product"`
	}{}
	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out.Product, nil
}

func (s *ProductServiceOp) GetSingleProduct(id graphql.ID) (*ProductQueryResult, error) {
	q := fmt.Sprintf(`
		query product($id: ID!) {
			product(id: $id){
				%s
				%s
				%s
			}
		}
	`, productBaseQuery, singleProductQueryVariant, singleProductQueryCollection)

	vars := map[string]interface{}{
		"id": id,
	}

	out := struct {
		Product *ProductQueryResult `json:"product"`
	}{}

	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out.Product, nil
}

func (s *ProductServiceOp) ListWithFields(first int, cursor, query, fields string) (*ProductsQueryResult, error) {
	if fields == "" {
		fields = `id`
	}

	q := fmt.Sprintf(`
		query products($first: Int!, $cursor: String, $query: String) {
			products(first: $first, after: $cursor, query:$query){
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
	out := &ProductsQueryResult{}

	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (s *ProductServiceOp) CreateBulk(products []*ProductCreate) error {
	for _, p := range products {
		err := s.Create(p)
		if err != nil {
			log.Warnf("Couldn't create product (%v): %s", p, err)
		}
	}

	return nil
}

func (s *ProductServiceOp) Create(product *ProductCreate) error {
	m := mutationProductCreate{}

	vars := map[string]interface{}{
		"input": product.ProductInput,
		"media": product.MediaInput,
	}
	err := s.client.gql.Mutate(context.Background(), &m, vars)
	if err != nil {
		return err
	}

	if len(m.ProductCreateResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.ProductCreateResult.UserErrors)
	}

	return nil
}

func (s *ProductServiceOp) UpdateBulk(products []*ProductUpdate) error {
	for _, p := range products {
		err := s.Update(p)
		if err != nil {
			log.Warnf("Couldn't update product (%v): %s", p, err)
		}
	}

	return nil
}

func (s *ProductServiceOp) Update(product *ProductUpdate) error {
	m := mutationProductUpdate{}

	vars := map[string]interface{}{
		"input": product.ProductInput,
	}
	err := s.client.gql.Mutate(context.Background(), &m, vars)
	if err != nil {
		return err
	}

	if len(m.ProductUpdateResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.ProductUpdateResult.UserErrors)
	}

	return nil
}

func (s *ProductServiceOp) DeleteBulk(products []*ProductDelete) error {
	for _, p := range products {
		err := s.Delete(p)
		if err != nil {
			log.Warnf("Couldn't delete product (%v): %s", p, err)
		}
	}

	return nil
}

func (s *ProductServiceOp) Delete(product *ProductDelete) error {
	m := mutationProductDelete{}

	vars := map[string]interface{}{
		"input": product.ProductInput,
	}
	err := s.client.gql.Mutate(context.Background(), &m, vars)
	if err != nil {
		return err
	}

	if len(m.ProductDeleteResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.ProductDeleteResult.UserErrors)
	}

	return nil
}
