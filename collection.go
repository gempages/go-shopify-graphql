package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-helper/errors"
	"github.com/gempages/go-shopify-graphql-model/graph/model"
	"github.com/gempages/go-shopify-graphql/graphql"
	log "github.com/sirupsen/logrus"
)

type CollectionService interface {
	List(ctx context.Context, opts ...QueryOption) ([]*model.Collection, error)
	ListWithFields(ctx context.Context, first int, cursor string, query string, fields string) (*model.CollectionConnection, error)

	Get(ctx context.Context, id string) (*model.Collection, error)
	GetSingleCollection(ctx context.Context, id string, cursor string) (*model.Collection, error)

	Create(ctx context.Context, collection model.CollectionInput) (output *model.Collection, err error)
	CreateBulk(ctx context.Context, collections []model.CollectionInput) error

	Update(ctx context.Context, collection model.CollectionInput) (output *model.Collection, err error)
}

type CollectionServiceOp struct {
	client *Client
}

var _ CollectionService = &CollectionServiceOp{}

type mutationCollectionCreate struct {
	CollectionCreateResult model.CollectionCreatePayload `graphql:"collectionCreate(input: $input)" json:"collectionCreate"`
}

type mutationCollectionUpdate struct {
	CollectionCreateResult model.CollectionUpdatePayload `graphql:"collectionUpdate(input: $input)" json:"collectionUpdate"`
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
    pageInfo{
      hasNextPage
    }
  }
`

var collectionBulkQuery = `
	id
	handle
	title
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

func (s *CollectionServiceOp) List(ctx context.Context, opts ...QueryOption) ([]*model.Collection, error) {
	b := &bulkQueryBuilder{
		operationName: "collections",
		fields:        collectionWithProductsBulkQuery,
	}
	for _, opt := range opts {
		opt(b)
	}
	q := b.Build()

	res := make([]*model.Collection, 0)
	err := s.client.BulkOperation.BulkQuery(ctx, q, &res)
	if err != nil {
		return nil, fmt.Errorf("bulk query: %w", err)
	}

	return res, nil
}

func (s *CollectionServiceOp) ListWithFields(ctx context.Context, first int, cursor, query, fields string) (*model.CollectionConnection, error) {
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
                pageInfo {
                      hasNextPage
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

	out := model.QueryRoot{}
	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out.Collections, nil
}

func (s *CollectionServiceOp) Get(ctx context.Context, id string) (*model.Collection, error) {
	var (
		out *model.Collection
		err error
	)
	out, err = s.getPage(ctx, id, "")
	if err != nil {
		return nil, err
	}

	nextPageData := out
	if out != nil && out.Products != nil && out.Products.PageInfo != nil {
		hasNextPage := out.Products.PageInfo.HasNextPage
		for hasNextPage && len(nextPageData.Products.Edges) > 0 {
			cursor := nextPageData.Products.Edges[len(nextPageData.Products.Edges)-1].Cursor
			nextPageData, err = s.getPage(ctx, id, cursor)
			if err != nil {
				return nil, err
			}
			out.Products.Edges = append(out.Products.Edges, nextPageData.Products.Edges...)
			hasNextPage = nextPageData.Products.PageInfo.HasNextPage
		}
	}

	return out, nil
}

func (s *CollectionServiceOp) getPage(ctx context.Context, id graphql.ID, cursor string) (*model.Collection, error) {
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

	out := model.QueryRoot{}
	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Collection == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "collection not found", nil)
	}

	return out.Collection, nil
}

func (s *CollectionServiceOp) GetSingleCollection(ctx context.Context, id string, cursor string) (*model.Collection, error) {
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

	out := model.QueryRoot{}
	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, err
	}

	if out.Collection == nil {
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "collection not found", nil)
	}

	return out.Collection, nil
}

func (s *CollectionServiceOp) CreateBulk(ctx context.Context, collections []model.CollectionInput) error {
	for _, c := range collections {
		_, err := s.client.Collection.Create(ctx, c)
		if err != nil {
			log.Warnf("Couldn't create collection (%v): %s", c, err)
		}
	}

	return nil
}

func (s *CollectionServiceOp) Create(ctx context.Context, collection model.CollectionInput) (output *model.Collection, err error) {
	m := mutationCollectionCreate{}

	vars := map[string]interface{}{
		"input": collection,
	}
	err = s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return
	}

	if len(m.CollectionCreateResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", m.CollectionCreateResult.UserErrors)
		return
	}

	return m.CollectionCreateResult.Collection, nil
}

func (s *CollectionServiceOp) Update(ctx context.Context, collection model.CollectionInput) (output *model.Collection, err error) {
	m := mutationCollectionUpdate{}

	vars := map[string]interface{}{
		"input": collection,
	}
	err = s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return
	}

	if len(m.CollectionCreateResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", m.CollectionCreateResult.UserErrors)
		return
	}

	return m.CollectionCreateResult.Collection, nil
}
