package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-shopify-graphql-model/graph/model"
)

type MetafieldDefinitionService interface {
	List(ctx context.Context, ownerType model.MetafieldOwnerType, opts ...QueryOption) (*model.MetafieldDefinitionConnection, error)
}

type MetafieldDefinitionServiceOp struct {
	client *Client
}

type metafieldDifinitionQueryOptionBuilder struct {
	ownerType model.MetafieldOwnerType
	fields    string
	query     *string
	first     int
	after     string
}

func (m *metafieldDifinitionQueryOptionBuilder) SetFields(fields string) {
	m.fields = fields
}

func (m *metafieldDifinitionQueryOptionBuilder) SetQuery(query string) {
	m.query = &query
}

func (m *metafieldDifinitionQueryOptionBuilder) SetFirst(first int) {
	m.first = first
}

func (m *metafieldDifinitionQueryOptionBuilder) SetAfter(after string) {
	m.after = after
}

func (m *metafieldDifinitionQueryOptionBuilder) Build() map[string]interface{} {
	vars := map[string]interface{}{
		"ownerType": m.ownerType,
		"first":     m.first,
	}

	if m.first == 0 {
		vars["first"] = 250
	}

	if m.after != "" {
		vars["after"] = m.after
	}

	return vars
}

func (s *MetafieldDefinitionServiceOp) List(ctx context.Context, ownerType model.MetafieldOwnerType, opts ...QueryOption) (*model.MetafieldDefinitionConnection, error) {
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

	out := model.QueryRoot{}
	queryOpt := metafieldDifinitionQueryOptionBuilder{
		ownerType: ownerType,
	}
	for _, opt := range opts {
		opt(&queryOpt)
	}

	vars := queryOpt.Build()
	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, fmt.Errorf("gql.QueryString: %w", err)
	}

	return out.MetafieldDefinitions, nil
}
