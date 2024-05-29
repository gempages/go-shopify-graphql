package shopify

import (
	"context"
	"fmt"
	"strings"

	"github.com/gempages/go-shopify-graphql-model/graph/model"
	log "github.com/sirupsen/logrus"

	"github.com/gempages/go-shopify-graphql/graphql"
)

type MetafieldService interface {
	ListAllShopMetafields(ctx context.Context) ([]*Metafield, error)
	ListShopMetafieldsByNamespace(ctx context.Context, namespace string) ([]*Metafield, error)

	GetShopMetafieldByKey(ctx context.Context, namespace, key string) (Metafield, error)

	Delete(ctx context.Context, metafield MetafieldDeleteInput) error
	DeleteBulk(ctx context.Context, metafield []MetafieldDeleteInput) error
	CreateBulk(ctx context.Context, inputs []*model.MetafieldsSetInput) ([]*model.Metafield, error)
}

type MetafieldServiceOp struct {
	client *Client
}

type Metafield struct {
	// The date and time when the metafield was created.
	CreatedAt DateTime `json:"createdAt,omitempty"`
	// The description of a metafield.
	Description graphql.String `json:"description,omitempty"`
	// Globally unique identifier.
	ID graphql.ID `json:"id,omitempty"`
	// The key name for a metafield.
	Key graphql.String `json:"key,omitempty"`
	// The ID of the corresponding resource in the REST Admin API.
	LegacyResourceID graphql.String `json:"legacyResourceId,omitempty"`
	// The namespace for a metafield.
	Namespace graphql.String `json:"namespace,omitempty"`
	// Owner type of a metafield visible to the Storefront API.
	OwnerType graphql.String `json:"ownerType,omitempty"`
	// The date and time when the metafield was updated.
	UpdatedAt DateTime `json:"updatedAt,omitempty"`
	// The value of a metafield.
	Value graphql.String `json:"value,omitempty"`
	// Represents the metafield value type.
	Type model.MetafieldValueType `json:"type,omitempty"`
}

type MetafieldDeleteInput struct {
	// The ID of the metafield to delete.
	ID graphql.ID `json:"id,omitempty"`
}

type mutationMetafieldDelete struct {
	MetafieldDeleteResult metafieldDeleteResult `graphql:"metafieldDelete(input: $input)" json:"metafieldDelete"`
}

type metafieldDeleteResult struct {
	DeletedID  string       `json:"deletedId,omitempty"`
	UserErrors []UserErrors `json:"userErrors"`
}

type mutationMetafieldCreateBulk struct {
	MetafieldCreateBulkPayload model.MetafieldsSetPayload `json:"metafieldCreateBulkPayload"`
}

var metafieldsSet = `
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields {
      key
      namespace
      value
      createdAt
      updatedAt
    }
    userErrors {
      field
      message
      code
    }
  }
}
`

func (s *MetafieldServiceOp) ListAllShopMetafields(ctx context.Context) ([]*Metafield, error) {
	q := `
		{
			shop{
				metafields{
					edges{
						node{
							createdAt
							description
							id
							key
							legacyResourceId
							namespace
							ownerType
							updatedAt
							value
							type
						}
					}
				}
			}
		}
`

	res := []*Metafield{}
	err := s.client.BulkOperation.BulkQuery(ctx, q, &res)
	if err != nil {
		return []*Metafield{}, err
	}

	return res, nil
}

func (s *MetafieldServiceOp) ListShopMetafieldsByNamespace(ctx context.Context, namespace string) ([]*Metafield, error) {
	q := `
		{
			shop{
				metafields(namespace: "$namespace"){
					edges{
						node{
							createdAt
							description
							id
							key
							legacyResourceId
							namespace
							ownerType
							updatedAt
							value
							type
						}
					}
				}
			}
		}
`
	q = strings.ReplaceAll(q, "$namespace", namespace)

	res := []*Metafield{}
	err := s.client.BulkOperation.BulkQuery(ctx, q, &res)
	if err != nil {
		return []*Metafield{}, err
	}

	return res, nil
}

func (s *MetafieldServiceOp) GetShopMetafieldByKey(ctx context.Context, namespace, key string) (Metafield, error) {
	var q struct {
		Shop struct {
			Metafield Metafield `graphql:"metafield(namespace: $namespace, key: $key)"`
		} `graphql:"shop"`
	}
	vars := map[string]interface{}{
		"namespace": graphql.String(namespace),
		"key":       graphql.String(key),
	}

	err := s.client.gql.Query(ctx, &q, vars)
	if err != nil {
		return Metafield{}, err
	}

	return q.Shop.Metafield, nil
}

func (s *MetafieldServiceOp) DeleteBulk(ctx context.Context, metafields []MetafieldDeleteInput) error {
	for _, m := range metafields {
		err := s.Delete(ctx, m)
		if err != nil {
			log.Warnf("Couldn't delete metafield (%v): %s", m, err)
		}
	}

	return nil
}

func (s *MetafieldServiceOp) Delete(ctx context.Context, metafield MetafieldDeleteInput) error {
	m := mutationMetafieldDelete{}

	vars := map[string]interface{}{
		"input": metafield,
	}
	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return err
	}

	if len(m.MetafieldDeleteResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.MetafieldDeleteResult.UserErrors)
	}

	return nil
}

func (s *DiscountServiceOp) CreateBulk(ctx context.Context, inputs []model.MetafieldsSetInput) ([]model.Metafield, error) {
	out := mutationMetafieldCreateBulk{}
	vars := map[string]interface{}{
		"metafields": inputs,
	}

	if err := s.client.gql.MutateString(ctx, metafieldsSet, vars, &out); err != nil {
		return nil, fmt.Errorf("gql.MutateString: %w", err)
	}

	if len(out.MetafieldCreateBulkPayload.UserErrors) >= 1 {
		return nil, fmt.Errorf("%+v", out.MetafieldCreateBulkPayload.UserErrors)
	}

	return out.MetafieldCreateBulkPayload.Metafields, nil
}
