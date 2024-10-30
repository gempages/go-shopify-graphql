package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-helper/errors"
	"github.com/gempages/go-shopify-graphql-model/graph/model"
)

type ListProductArgs struct {
	Fields  string
	Query   string
	First   int
	After   string
	Reverse bool
	SortKey string
}

type ProductService interface {
	List(ctx context.Context, opts ...QueryOption) ([]*model.Product, error)
	ListWithFields(ctx context.Context, req *ListProductArgs) (*model.ProductConnection, error)

	Get(ctx context.Context, id string) (*model.Product, error)
	GetWithFields(ctx context.Context, id string, fields string) (*model.Product, error)
	GetSingleProductCollection(ctx context.Context, id string, cursor string) (*model.Product, error)

	Create(ctx context.Context, product model.ProductInput, media []model.CreateMediaInput) (output *model.Product, err error)
	Update(ctx context.Context, product model.ProductInput) (output *model.Product, err error)
	Delete(ctx context.Context, product model.ProductDeleteInput) (deletedID *string, err error)
}

type ProductServiceOp struct {
	client *Client
}

var _ ProductService = &ProductServiceOp{}

type mutationProductCreate struct {
	ProductCreateResult model.ProductCreatePayload `graphql:"productCreate(input: $input, media: $media)" json:"productCreate"`
}

type mutationProductUpdate struct {
	ProductUpdateResult model.ProductUpdatePayload `graphql:"productUpdate(input: $input)" json:"productUpdate"`
}

type mutationProductDelete struct {
	ProductDeleteResult model.ProductDeletePayload `graphql:"productDelete(input: $input)" json:"productDelete"`
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
		optionValues {
			id
			name
		}
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
	variants(first: 250, after: $variantAfter) {
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
				position
				inventoryItem {
                    tracked
                }
			}
		}
		pageInfo{
			hasNextPage
			endCursor
		}
	}
`, productBaseQuery)

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
				ownerType
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
	media {
		edges {
			node {
				__typename
				mediaContentType
				...on MediaImage {
					id
					alt
					mimeType
					image {
                		height
                		src
                		width
					}
				}
				...on Model3d {
					id
					alt
					originalSource {
						url
						format
						filesize
						mimeType
					}
					preview {
						image {
							src
						}
					}
				}
				...on Video {
					id
					alt
					duration
					originalSource {
						url
						format
						mimeType
 						height
						width
					}
					preview {
						image {
							src
						}
					}
				}
				...on ExternalVideo {
					id
					originUrl
					embedUrl
					preview {
						image {
							src
						}
					}
				}
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
				position
				inventoryItem {
                    tracked
                }
			}
		}
	}
`, productBaseQuery)

func (s *ProductServiceOp) List(ctx context.Context, opts ...QueryOption) ([]*model.Product, error) {
	b := &bulkQueryBuilder{
		operationName: "products",
		fields:        productBulkQuery,
	}
	for _, opt := range opts {
		opt(b)
	}
	q := b.Build()

	res := make([]*model.Product, 0)
	err := s.client.BulkOperation.BulkQuery(ctx, q, &res)
	if err != nil {
		return nil, fmt.Errorf("bulk query: %w", err)
	}

	return res, nil
}

func (s *ProductServiceOp) ListWithFields(ctx context.Context, req *ListProductArgs) (*model.ProductConnection, error) {
	if req == nil {
		req = &ListProductArgs{}
	}

	if req.Fields == "" {
		req.Fields = `id`
	}

	if req.SortKey == "" {
		req.SortKey = `ID`
	}

	q := fmt.Sprintf(`
		query products ($first: Int!, $after: String, $query: String, $sortKey: ProductSortKeys, $reverse: Boolean!) {
			products (first: $first, after: $after, query: $query, sortKey: $sortKey, reverse: $reverse) {
				edges {
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
	`, req.Fields)

	vars := map[string]interface{}{
		"first": req.First,
	}
	if req.After != "" {
		vars["after"] = req.After
	}
	if req.Query != "" {
		vars["query"] = req.Query
	}
	if req.SortKey != "" {
		vars["sortKey"] = req.SortKey
	}
	vars["reverse"] = req.Reverse

	out := model.QueryRoot{}

	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out.Products, nil
}

func (s *ProductServiceOp) Get(ctx context.Context, id string) (*model.Product, error) {
	out, err := s.getPage(ctx, id, nil)
	if err != nil {
		return nil, err
	}

	nextPageData := out
	if out != nil && out.Variants != nil && out.Variants.PageInfo != nil {
		hasNextPage := out.Variants.PageInfo.HasNextPage
		for hasNextPage && nextPageData.Variants.PageInfo.EndCursor != nil {
			cursor := nextPageData.Variants.PageInfo.EndCursor
			nextPageData, err = s.getPage(ctx, id, cursor)
			if err != nil {
				return nil, err
			}
			out.Variants.Edges = append(out.Variants.Edges, nextPageData.Variants.Edges...)
			hasNextPage = nextPageData.Variants.PageInfo.HasNextPage
		}
	}

	return out, nil
}

func (s *ProductServiceOp) getPage(ctx context.Context, id string, variantAfter *string) (*model.Product, error) {
	q := fmt.Sprintf(`
		query product($id: ID!, $variantAfter: String) {
			product(id: $id){
				%s
			}
		}
	`, productQuery)

	vars := map[string]interface{}{
		"id":           id,
		"variantAfter": variantAfter,
	}

	out := model.QueryRoot{}
	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Product == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "product not found")
	}

	return out.Product, nil
}

func (s *ProductServiceOp) GetWithFields(ctx context.Context, id string, fields string) (*model.Product, error) {
	if fields == "" {
		fields = `id`
	}
	q := fmt.Sprintf(`
		query product($id: ID!) {
		  product(id: $id){
			%s
		  }
		}`, fields)

	vars := map[string]interface{}{
		"id": id,
	}

	out := model.QueryRoot{}
	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Product == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "product not found")
	}

	return out.Product, nil
}

func (s *ProductServiceOp) GetSingleProductCollection(ctx context.Context, id string, cursor string) (*model.Product, error) {
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

	out := model.QueryRoot{}
	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Product == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "product not found")
	}

	return out.Product, nil
}

func (s *ProductServiceOp) Create(ctx context.Context, product model.ProductInput, media []model.CreateMediaInput) (output *model.Product, err error) {
	m := mutationProductCreate{}

	vars := map[string]interface{}{
		"input": product,
		"media": media,
	}
	err = s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return
	}

	if len(m.ProductCreateResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", m.ProductCreateResult.UserErrors)
		return
	}

	return m.ProductCreateResult.Product, nil
}

func (s *ProductServiceOp) Update(ctx context.Context, product model.ProductInput) (output *model.Product, err error) {
	m := mutationProductUpdate{}

	vars := map[string]interface{}{
		"input": product,
	}
	err = s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return
	}

	if len(m.ProductUpdateResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", m.ProductUpdateResult.UserErrors)
		return
	}

	return m.ProductUpdateResult.Product, nil
}

func (s *ProductServiceOp) Delete(ctx context.Context, product model.ProductDeleteInput) (deletedID *string, err error) {
	m := mutationProductDelete{}

	vars := map[string]interface{}{
		"input": product,
	}
	err = s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return
	}

	if len(m.ProductDeleteResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", m.ProductDeleteResult.UserErrors)
		return
	}

	return m.ProductDeleteResult.DeletedProductID, nil
}
