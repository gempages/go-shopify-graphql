package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-shopify-graphql-model/graph/model"

	"github.com/gempages/go-shopify-graphql/graphql"
)

type MetafieldDefinitionService interface {
	List(ctx context.Context, ownerType model.MetafieldOwnerType, opts ListOptions) (*model.MetafieldDefinitionConnection, error)
}

type MetafieldDefinitionServiceOp struct {
	client *Client
}

func (s *MetafieldDefinitionServiceOp) List(ctx context.Context, ownerType model.MetafieldOwnerType, opts ListOptions) (*model.MetafieldDefinitionConnection, error) {
	q := `
		query metafieldDefinitions($first: Int, $after: String, $ownerType: MetafieldOwnerType!) {
			metafieldDefinitions(first: $first, after: $after, ownerType: $ownerType) {
				edges {
					node {
						id
						name
						namespace
						key
						description
						ownerType
						type {
							category
							name
						}
					}
					cursor
				}
				pageInfo {
					hasNextPage
					endCursor
				}
			}
		}
`
	if opts.First == 0 {
		opts.First = 250
	}

	out := model.QueryRoot{}
	vars := map[string]interface{}{
		"first":     graphql.Int(opts.First),
		"ownerType": graphql.String(ownerType),
	}

	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, fmt.Errorf("gql.QueryString: %w", err)
	}

	return out.MetafieldDefinitions, nil
}
