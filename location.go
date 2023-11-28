package shopify

import (
	"context"

	"github.com/gempages/go-shopify-graphql/graphql"
)

type LocationService interface {
	Get(ctx context.Context, id graphql.ID) (*Location, error)
}

type LocationServiceOp struct {
	client *Client
}

type Location struct {
	ID   graphql.ID     `json:"id,omitempty"`
	Name graphql.String `json:"name,omitempty"`
}

func (s *LocationServiceOp) Get(ctx context.Context, id graphql.ID) (*Location, error) {
	q := `query location($id: ID!) {
		location(id: $id){
			id
			name
		}
	}`

	vars := map[string]interface{}{
		"id": id,
	}

	out := struct {
		Location *Location `json:"location"`
	}{}
	err := s.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out.Location, nil
}
