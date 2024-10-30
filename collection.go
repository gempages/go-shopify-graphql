package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-helper/errors"
	"github.com/gempages/go-shopify-graphql-model/graph/model"
	log "github.com/sirupsen/logrus"

	"github.com/gempages/go-shopify-graphql/graphql"
)

type ListCollectionArgs struct {
	Fields  string
	Query   string
	First   int
	After   string
	Reverse bool
	SortKey string
}

type CollectionService interface {
	List(ctx context.Context, opts ...QueryOption) ([]*model.Collection, error)
	ListWithFields(ctx context.Context, args *ListCollectionArgs) (*model.CollectionConnection, error)

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

var collectionWithProductsBulkQuery = `
	id
	handle
	title
	updatedAt
	description
	descriptionHtml
	templateSuffix
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

func (s *CollectionServiceOp) ListWithFields(ctx context.Context, args *ListCollectionArgs) (*model.CollectionConnection, error) {
	if args == nil {
		args = &ListCollectionArgs{}
	}

	if args.Fields == "" {
		args.Fields = `id`
	}

	if args.SortKey == "" {
		args.SortKey = `ID`
	}

	q := fmt.Sprintf(`
		query collections($first: Int!, $after: String, $query: String, $sortKey: CollectionSortKeys, $reverse: Boolean!) {
			collections(first: $first, after: $after, query:$query, sortKey: $sortKey, reverse: $reverse) {
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
	`, args.Fields)

	vars := map[string]interface{}{
		"first": args.First,
	}
	if args.After != "" {
		vars["after"] = args.After
	}
	if args.Query != "" {
		vars["query"] = args.Query
	}
	if args.SortKey != "" {
		vars["sortKey"] = args.SortKey
	}
	vars["reverse"] = args.Reverse

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
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "collection not found")
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
		return nil, errors.NewNotExistsError(errors.ErrorResourceNotFound, "collection not found")
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
