package shopify

import (
	"context"
	"fmt"
	"strings"

	"github.com/gempages/go-helper/errors"
	"github.com/gempages/go-shopify-graphql-model/graph/model"
)

type ProductService interface {
	List(query string) ([]*model.Product, error)
	ListAll() ([]*model.Product, error)
	ListWithFields(query string, fields string, first int, after string) (*model.ProductConnection, error)

	Get(id string) (*model.Product, error)
	GetWithFields(id string, fields string) (*model.Product, error)
	GetSingleProductCollection(id string, cursor string) (*model.Product, error)
	GetSingleProductVariant(id string, cursor string) (*model.Product, error)
	GetSingleProduct(id string) (*model.Product, error)

	Create(product model.ProductInput, media []model.CreateMediaInput) (output *model.Product, err error)
	Update(product model.ProductInput) (output *model.Product, err error)
	Delete(product model.ProductDeleteInput) (deletedID *string, err error)
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
		position
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
		position
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
				position
			}
		}
		pageInfo{
			hasNextPage
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
				inventoryManagement
				weightUnit
				weight
				position
			}
		}
	}
`, productBaseQuery)

func (s *ProductServiceOp) ListAll() ([]*model.Product, error) {
	q := fmt.Sprintf(`
		query products{
			products{
				edges{
					node{
						%s
					}
				}
			}
		}
	`, productBulkQuery)

	res := make([]*model.Product, 0)
	err := s.client.BulkOperation.BulkQuery(q, &res)
	if err != nil {
		return nil, fmt.Errorf("bulk query: %w", err)
	}

	return res, nil
}

func (s *ProductServiceOp) List(query string) ([]*model.Product, error) {
	q := fmt.Sprintf(`
		query products {
			products(query: "$query") {
				edges{
					node{
						%s
					}
				}
			}
		}
	`, productBulkQuery)

	q = strings.ReplaceAll(q, "$query", query)

	res := make([]*model.Product, 0)
	err := s.client.BulkOperation.BulkQuery(q, &res)
	if err != nil {
		return nil, fmt.Errorf("bulk query: %w", err)
	}

	return res, nil
}

func (s *ProductServiceOp) ListWithFields(query, fields string, first int, after string) (*model.ProductConnection, error) {
	if fields == "" {
		fields = `id`
	}

	q := fmt.Sprintf(`
		query products ($first: Int!, $after: String, $query: String) {
			products (first: $first, after: $after, query: $query) {
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
	`, fields)

	vars := map[string]interface{}{
		"first": first,
	}
	if after != "" {
		vars["after"] = after
	}
	if query != "" {
		vars["query"] = query
	}
	out := model.QueryRoot{}

	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out.Products, nil
}

func (s *ProductServiceOp) Get(id string) (*model.Product, error) {
	out, err := s.getPage(id, "")
	if err != nil {
		return nil, err
	}

	nextPageData := out
	if out != nil && out.Variants != nil && out.Variants.PageInfo != nil {
		hasNextPage := out.Variants.PageInfo.HasNextPage
		for hasNextPage && len(nextPageData.Variants.Edges) > 0 {
			cursor := nextPageData.Variants.Edges[len(nextPageData.Variants.Edges)-1].Cursor
			nextPageData, err := s.getPage(id, cursor)
			if err != nil {
				return nil, err
			}
			out.Variants.Edges = append(out.Variants.Edges, nextPageData.Variants.Edges...)
			hasNextPage = nextPageData.Variants.PageInfo.HasNextPage
		}
	}

	return out, nil
}

func (s *ProductServiceOp) getPage(id string, cursor string) (*model.Product, error) {
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

	out := model.QueryRoot{}
	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Product == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "product not found", nil)
	}

	return out.Product, nil
}

func (s *ProductServiceOp) GetWithFields(id string, fields string) (*model.Product, error) {
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
	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Product == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "product not found", nil)
	}

	return out.Product, nil
}

func (s *ProductServiceOp) GetSingleProductCollection(id string, cursor string) (*model.Product, error) {
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
	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Product == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "product not found", nil)
	}

	return out.Product, nil
}

func (s *ProductServiceOp) GetSingleProductVariant(id string, cursor string) (*model.Product, error) {
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

	out := model.QueryRoot{}
	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Product == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "product not found", nil)
	}

	return out.Product, nil
}

func (s *ProductServiceOp) GetSingleProduct(id string) (*model.Product, error) {
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

	out := model.QueryRoot{}
	err := s.client.gql.QueryString(context.Background(), q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Product == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "product not found", nil)
	}

	return out.Product, nil
}

func (s *ProductServiceOp) Create(product model.ProductInput, media []model.CreateMediaInput) (output *model.Product, err error) {
	m := mutationProductCreate{}

	vars := map[string]interface{}{
		"input": product,
		"media": media,
	}
	err = s.client.gql.Mutate(context.Background(), &m, vars)
	if err != nil {
		return
	}

	if len(m.ProductCreateResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", m.ProductCreateResult.UserErrors)
		return
	}

	return m.ProductCreateResult.Product, nil
}

func (s *ProductServiceOp) Update(product model.ProductInput) (output *model.Product, err error) {
	m := mutationProductUpdate{}

	vars := map[string]interface{}{
		"input": product,
	}
	err = s.client.gql.Mutate(context.Background(), &m, vars)
	if err != nil {
		return
	}

	if len(m.ProductUpdateResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", m.ProductUpdateResult.UserErrors)
		return
	}

	return m.ProductUpdateResult.Product, nil
}

func (s *ProductServiceOp) Delete(product model.ProductDeleteInput) (deletedID *string, err error) {
	m := mutationProductDelete{}

	vars := map[string]interface{}{
		"input": product,
	}
	err = s.client.gql.Mutate(context.Background(), &m, vars)
	if err != nil {
		return
	}

	if len(m.ProductDeleteResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", m.ProductDeleteResult.UserErrors)
		return
	}

	return m.ProductDeleteResult.DeletedProductID, nil
}
