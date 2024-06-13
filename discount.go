package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-shopify-graphql-model/graph/model"
)

type DiscountService interface {
	AutomaticAppCreate(ctx context.Context, discount model.DiscountAutomaticAppInput) (*model.DiscountAutomaticApp, error)
	AutomaticAppUpdate(ctx context.Context, discountBaseID string, discount DiscountAutomaticAppInput) (*model.DiscountAutomaticApp, error)
	AutomaticDelete(ctx context.Context, discountBaseID string) error
	AutomaticActivate(ctx context.Context, discountBaseID string) (*model.DiscountAutomaticNode, error)
	AutomaticDeactivate(ctx context.Context, discountBaseID string) (*model.DiscountAutomaticNode, error)
}

type DiscountServiceOp struct {
	client *Client
}

var _ DiscountService = &DiscountServiceOp{}

type mutationDiscountAutomaticAppCreate struct {
	DiscountAutomaticCreateAppPayload model.DiscountAutomaticAppCreatePayload `json:"discountAutomaticAppCreate"`
}

type mutationDiscountAutomaticAppUpdate struct {
	DiscountAutomaticAppUpdatePayload model.DiscountAutomaticAppUpdatePayload `json:"discountAutomaticAppUpdate"`
}

type mutationDiscountAutomaticDelete struct {
	DiscountAutomaticDeletePayload model.DiscountAutomaticDeletePayload `json:"discountAutomaticDelete"`
}

type mutationDiscountAutomaticActivate struct {
	DiscountAutomaticActivatePayload model.DiscountAutomaticActivatePayload `json:"discountAutomaticActivate"`
}

type mutationDiscountAutomaticDeactivate struct {
	DiscountAutomaticDeactivatePayload model.DiscountAutomaticDeactivatePayload `json:"discountAutomaticDeactivate"`
}

type DiscountAutomaticAppInput struct {
	model.DiscountAutomaticAppInput
	ClearEndsAt bool `json:"-"`
}

func (i *DiscountAutomaticAppInput) ToMap() map[string]any {
	result := make(map[string]any)
	if i.ClearEndsAt {
		result["endsAt"] = nil
	}
	if i.EndsAt != nil {
		result["endsAt"] = i.EndsAt
	}
	if i.Title != nil {
		result["title"] = i.Title
	}
	if i.CombinesWith != nil {
		result["combinesWith"] = i.CombinesWith
	}
	if i.Metafields != nil {
		result["metafields"] = i.Metafields
	}
	return result
}

var discountAutomaticAppCreate = `
mutation discountAutomaticAppCreate($automaticAppDiscount: DiscountAutomaticAppInput!) {
  discountAutomaticAppCreate(automaticAppDiscount: $automaticAppDiscount) {
    userErrors {
      field
      code
      message
      extraInfo
    }
    automaticAppDiscount {
      discountId
      title
      startsAt
      endsAt
      status
      appDiscountType {
        appKey
        functionId
      }
      combinesWith {
        orderDiscounts
        productDiscounts
        shippingDiscounts
      }
    }
  }
}
`

var discountAutomaticAppUpdate = `
mutation discountAutomaticAppUpdate($automaticAppDiscount: DiscountAutomaticAppInput!, $id: ID!) {
  discountAutomaticAppUpdate(automaticAppDiscount: $automaticAppDiscount, id: $id) {
    automaticAppDiscount {
      discountId
      title
      startsAt
      endsAt
      status
      appDiscountType {
        appKey
        functionId
      }
      combinesWith {
        orderDiscounts
        productDiscounts
        shippingDiscounts
      }
    }
    userErrors {
      field
      code
      message
      extraInfo
    }
  }
}
`

var discountAutomaticDelete = `
mutation discountAutomaticDelete($id: ID!) {
  discountAutomaticDelete(id: $id) {
    deletedAutomaticDiscountId
    userErrors {
      field
      code
      message
      extraInfo
    }
  }
}
`

var discountAutomaticActivate = `
mutation discountAutomaticActivate($id: ID!) {
  discountAutomaticActivate(id: $id) {
    automaticDiscountNode {
      automaticDiscount {
        ... on DiscountAutomaticApp {
          title
          status
          startsAt
          endsAt
        }
      }
    }
    userErrors {
      field
      message
      code
      extraInfo
    }
  }
}
`

var discountAutomaticDeactivate = `
mutation discountAutomaticDeactivate($id: ID!) {
  discountAutomaticDeactivate(id: $id) {
    automaticDiscountNode {
      automaticDiscount {
        ... on DiscountAutomaticApp {
          discountId
          title
          status
          startsAt
          endsAt 
        }
      }
    }
    userErrors {
      field
      message
      code
      extraInfo
    }
  }
}
`

func (s *DiscountServiceOp) AutomaticAppCreate(ctx context.Context, input model.DiscountAutomaticAppInput) (*model.DiscountAutomaticApp, error) {
	out := mutationDiscountAutomaticAppCreate{}
	vars := map[string]any{
		"automaticAppDiscount": input,
	}

	if err := s.client.gql.MutateString(ctx, discountAutomaticAppCreate, vars, &out); err != nil {
		return nil, fmt.Errorf("gql.MutateString: %w", err)
	}

	if len(out.DiscountAutomaticCreateAppPayload.UserErrors) > 0 {
		return nil, parseUserErrors(out.DiscountAutomaticCreateAppPayload.UserErrors)
	}

	return out.DiscountAutomaticCreateAppPayload.AutomaticAppDiscount, nil
}

func (s *DiscountServiceOp) AutomaticAppUpdate(ctx context.Context, discountBaseID string, input DiscountAutomaticAppInput) (*model.DiscountAutomaticApp, error) {
	out := mutationDiscountAutomaticAppUpdate{}
	vars := map[string]any{
		"id":                   discountBaseID,
		"automaticAppDiscount": input.ToMap(),
	}
	if err := s.client.gql.MutateString(ctx, discountAutomaticAppUpdate, vars, &out); err != nil {
		return nil, fmt.Errorf("gql.MutateString: %w", err)
	}

	if len(out.DiscountAutomaticAppUpdatePayload.UserErrors) > 0 {
		return nil, parseUserErrors(out.DiscountAutomaticAppUpdatePayload.UserErrors)
	}

	return out.DiscountAutomaticAppUpdatePayload.AutomaticAppDiscount, nil
}

func (s *DiscountServiceOp) AutomaticDelete(ctx context.Context, discountBaseID string) error {
	out := mutationDiscountAutomaticDelete{}
	vars := map[string]any{
		"id": discountBaseID,
	}
	if err := s.client.gql.MutateString(ctx, discountAutomaticDelete, vars, &out); err != nil {
		return fmt.Errorf("gql.MutateString: %w", err)
	}

	if len(out.DiscountAutomaticDeletePayload.UserErrors) > 0 {
		return parseUserErrors(out.DiscountAutomaticDeletePayload.UserErrors)
	}

	return nil
}

func (s *DiscountServiceOp) AutomaticActivate(ctx context.Context, discountBaseID string) (*model.DiscountAutomaticNode, error) {
	out := mutationDiscountAutomaticActivate{}
	vars := map[string]any{
		"id": discountBaseID,
	}

	if err := s.client.gql.MutateString(ctx, discountAutomaticActivate, vars, &out); err != nil {
		return nil, fmt.Errorf("gql.MutateString: %w", err)
	}

	if len(out.DiscountAutomaticActivatePayload.UserErrors) > 0 {
		return nil, parseUserErrors(out.DiscountAutomaticActivatePayload.UserErrors)
	}

	return out.DiscountAutomaticActivatePayload.AutomaticDiscountNode, nil
}

func (s *DiscountServiceOp) AutomaticDeactivate(ctx context.Context, discountBaseID string) (*model.DiscountAutomaticNode, error) {
	out := mutationDiscountAutomaticDeactivate{}
	vars := map[string]any{
		"id": discountBaseID,
	}

	if err := s.client.gql.MutateString(ctx, discountAutomaticDeactivate, vars, &out); err != nil {
		return nil, fmt.Errorf("gql.MutateString: %w", err)
	}

	if len(out.DiscountAutomaticDeactivatePayload.UserErrors) > 0 {
		return nil, parseUserErrors(out.DiscountAutomaticDeactivatePayload.UserErrors)
	}

	return out.DiscountAutomaticDeactivatePayload.AutomaticDiscountNode, nil
}

func parseUserErrors(errors []model.DiscountUserError) error {
	for _, userErr := range errors {
		if userErr.Code == nil {
			continue
		}
		switch *userErr.Code {
		case model.DiscountErrorCodeInvalid:
			if len(userErr.GetField()) >= 1 {
				return NewDiscountErrorf(model.DiscountErrorCodeInvalid, "%s: %s", userErr.GetField()[len(userErr.Field)-1], userErr.Message)
			}
			return NewDiscountError(model.DiscountErrorCodeInvalid, userErr.Message)
		case model.DiscountErrorCodeMaxAppDiscounts:
			return NewDiscountError(model.DiscountErrorCodeMaxAppDiscounts, userErr.Message)
		}
	}
	return fmt.Errorf("%+v", errors)
}
