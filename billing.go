package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-shopify-graphql-model/graph/model"

	"github.com/gempages/go-shopify-graphql/graphql"
)

type BillingService interface {
	AppSubscriptionCreate(ctx context.Context, input AppSubscriptionCreateInput) (*model.AppSubscriptionCreatePayload, error)
	AppSubscriptionCancel(ctx context.Context, id graphql.ID, prorate graphql.Boolean) (*model.AppSubscriptionCancelPayload, error)
	AppSubscriptionLineItemUpdate(ctx context.Context, id string, cappedAmount model.MoneyInput) (*model.AppSubscriptionLineItemUpdatePayload, error)
	AppUsageRecordCreate(ctx context.Context, input *model.AppUsageRecord) (*model.AppUsageRecordCreatePayload, error)
	AppPurchaseOneTimeCreate(ctx context.Context, input *AppPurchaseOneTimeCreateInput) (*model.AppPurchaseOneTimeCreatePayload, error)
	AppSubscriptionTrialExtend(ctx context.Context, appSubscriptionID string, days int) (*model.AppSubscriptionTrialExtendPayload, error)
}

type BillingServiceOp struct {
	client *Client
}

type AppSubscriptionCreateInput struct {
	LineItems           []model.AppSubscriptionLineItemInput      `json:"lineItems,omitempty"`
	Name                string                                    `json:"name,omitempty"`
	ReturnUrl           string                                    `json:"returnUrl,omitempty"`
	ReplacementBehavior *model.AppSubscriptionReplacementBehavior `json:"replacementBehavior,omitempty"`
	Test                *bool                                     `json:"test,omitempty" `
	TrialDays           *int                                      `json:"trialDays,omitempty"`
}

type MutationAppSubscriptionCreate struct {
	AppSubscriptionCreatePayload model.AppSubscriptionCreatePayload `json:"appSubscriptionCreate"`
}

var appSubscriptionCreate = `
mutation AppSubscriptionCreate($name: String!, $lineItems: [AppSubscriptionLineItemInput!]!, $returnUrl: URL!, $test: Boolean, $trialDays: Int, $replacementBehavior: AppSubscriptionReplacementBehavior) {
appSubscriptionCreate(name: $name, returnUrl: $returnUrl, lineItems: $lineItems, test: $test, trialDays: $trialDays, replacementBehavior: $replacementBehavior) {
    userErrors {
        field
        message
    }
    confirmationUrl
    appSubscription {
        __typename
        id
        createdAt
        currentPeriodEnd
        name
        returnUrl
        status
        trialDays
        test
        lineItems {
            id
            plan {
                pricingDetails {
                    ... on AppPricingDetails {
                        ... on AppRecurringPricing {
                            __typename
                            price {
                                amount
                                currencyCode
                            }
                            interval
                            discount {
                                ... on AppSubscriptionDiscount {
                                    __typename
                                    durationLimitInIntervals
                                    priceAfterDiscount {
                                        amount
                                        currencyCode
                                    }
                                    remainingDurationInIntervals
                                    value {
                                        ... on AppSubscriptionDiscountAmount {
                                            __typename
                                            amount {
                                                amount
                                                currencyCode
                                            }
                                        }
                                        ... on AppSubscriptionDiscountPercentage {
                                            __typename
                                            percentage
                                        }
                                    }
                                }
                            }
                        }
                        ... on AppUsagePricing {
                            __typename
                            balanceUsed {
                                amount
                                currencyCode
                            }
                            cappedAmount {
                                amount
                                currencyCode
                            }
                            interval
                            terms
                        }
                    }
                }
            }
        }
    }
  }
}
`

func (instance *BillingServiceOp) AppSubscriptionCreate(ctx context.Context, input AppSubscriptionCreateInput) (*model.AppSubscriptionCreatePayload, error) {
	m := MutationAppSubscriptionCreate{}
	vars := map[string]any{
		"lineItems":           input.LineItems,
		"name":                input.Name,
		"returnUrl":           input.ReturnUrl,
		"test":                input.Test,
		"trialDays":           input.TrialDays,
		"replacementBehavior": input.ReplacementBehavior,
	}
	err := instance.client.gql.MutateString(ctx, appSubscriptionCreate, vars, &m)
	if err != nil {
		return nil, err
	}

	if len(m.AppSubscriptionCreatePayload.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.AppSubscriptionCreatePayload.UserErrors)
	}

	return &m.AppSubscriptionCreatePayload, nil
}

type MutationAppSubscriptionCancel struct {
	AppSubscriptionCancelPayload model.AppSubscriptionCancelPayload `graphql:"appSubscriptionCancel(id: $id, prorate: $prorate)" json:"appSubscriptionCancel"`
}

func (instance *BillingServiceOp) AppSubscriptionCancel(ctx context.Context, id graphql.ID, prorate graphql.Boolean) (*model.AppSubscriptionCancelPayload, error) {
	m := MutationAppSubscriptionCancel{}

	vars := map[string]any{
		"id":      id,
		"prorate": prorate,
	}
	err := instance.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return nil, err
	}

	if len(m.AppSubscriptionCancelPayload.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.AppSubscriptionCancelPayload.UserErrors)
	}
	return &m.AppSubscriptionCancelPayload, nil
}

type mutationAppSubscriptionLineItemUpdate struct {
	AppSubscriptionLineItemUpdatePayload model.AppSubscriptionLineItemUpdatePayload `json:"appSubscriptionLineItemUpdate"`
}

var appSubscriptionLineItemUpdate = `
mutation appSubscriptionLineItemUpdate($cappedAmount: MoneyInput!, $id: ID!) {
  appSubscriptionLineItemUpdate(cappedAmount: $cappedAmount, id: $id) {
    userErrors {
      field
      message
    }
    confirmationUrl
    appSubscription {
      createdAt
      currentPeriodEnd
      id
      name
      returnUrl
      status
      test
      trialDays
    }
  }
}
`

func (instance *BillingServiceOp) AppSubscriptionLineItemUpdate(ctx context.Context, id string, cappedAmount model.MoneyInput) (*model.AppSubscriptionLineItemUpdatePayload, error) {
	m := mutationAppSubscriptionLineItemUpdate{}
	vars := map[string]any{
		"id":           id,
		"cappedAmount": cappedAmount,
	}
	err := instance.client.gql.MutateString(ctx, appSubscriptionLineItemUpdate, vars, &m)
	if err != nil {
		return nil, err
	}
	if len(m.AppSubscriptionLineItemUpdatePayload.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.AppSubscriptionLineItemUpdatePayload.UserErrors)
	}
	return &m.AppSubscriptionLineItemUpdatePayload, nil
}

type mutationAppUsageRecordCreate struct {
	AppUsageRecordCreatePayload model.AppUsageRecordCreatePayload `json:"appUsageRecordCreate"`
}

var appSubscriptionLineItemPlan = fmt.Sprintf(`
  plan {
    %s
  }
`, pricingDetails)

var pricingDetails = fmt.Sprintf(`
  pricingDetails {
	%s
	%s
  }
`, appRecurringPricing, appUsagePricing)

var appRecurringPricing = `
  ... on AppRecurringPricing {
    __typename
    price {
	  amount
	  currencyCode
    }
    interval
  }
`

var appUsagePricing = `
  ... on AppUsagePricing {
    __typename
    balanceUsed {
	  amount
	  currencyCode
    }
    cappedAmount {
	  amount
	  currencyCode
    }
    interval
    terms
  }
`

var appUsageRecordCreate = fmt.Sprintf(`
mutation appUsageRecordCreate(
  $description: String!
  $price: MoneyInput!
  $subscriptionLineItemId: ID!
  $idempotencyKey: String
) {
  appUsageRecordCreate(
    description: $description
    price: $price
    subscriptionLineItemId: $subscriptionLineItemId
    idempotencyKey: $idempotencyKey
  ) {
    userErrors {
      field
      message
    }
    appUsageRecord {
      id
      idempotencyKey
      subscriptionLineItem {
        id
		%s
      }
    }
  }
}
`, appSubscriptionLineItemPlan)

func (instance *BillingServiceOp) AppUsageRecordCreate(ctx context.Context, input *model.AppUsageRecord) (*model.AppUsageRecordCreatePayload, error) {
	m := mutationAppUsageRecordCreate{}
	vars := map[string]any{
		"description":            input.Description,
		"idempotencyKey":         input.IdempotencyKey,
		"price":                  input.Price,
		"subscriptionLineItemId": input.ID,
	}
	err := instance.client.gql.MutateString(ctx, appUsageRecordCreate, vars, &m)
	if err != nil {
		return nil, err
	}
	if len(m.AppUsageRecordCreatePayload.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.AppUsageRecordCreatePayload.UserErrors)
	}
	return &m.AppUsageRecordCreatePayload, nil
}

type AppPurchaseOneTimeCreateInput struct {
	Name      string           `json:"name,omitempty"`
	Price     model.MoneyInput `json:"price,omitempty"`
	ReturnUrl string           `json:"returnUrl,omitempty"`
	Test      *bool            `json:"test,omitempty"`
}

type MutationAppPurchaseOneTimeCreate struct {
	AppPurchaseOneTimeCreatePayload model.AppPurchaseOneTimeCreatePayload `graphql:"appPurchaseOneTimeCreate(name: $name, price: $price, returnUrl: $returnUrl, test: $test)" json:"appPurchaseOneTimeCreate"`
}

func (instance *BillingServiceOp) AppPurchaseOneTimeCreate(ctx context.Context, input *AppPurchaseOneTimeCreateInput) (*model.AppPurchaseOneTimeCreatePayload, error) {
	m := MutationAppPurchaseOneTimeCreate{}

	if input != nil {
		vars := map[string]any{
			"name":      input.Name,
			"price":     input.Price,
			"returnUrl": input.ReturnUrl,
			"test":      input.Test,
		}
		err := instance.client.gql.Mutate(ctx, &m, vars)
		if err != nil {
			return nil, err
		}

		if len(m.AppPurchaseOneTimeCreatePayload.UserErrors) > 0 {
			return nil, fmt.Errorf("%+v", m.AppPurchaseOneTimeCreatePayload.UserErrors)
		}
	}
	return &m.AppPurchaseOneTimeCreatePayload, nil
}

type MutationAppSubscriptionTrialExtend struct {
	AppSubscriptionTrialExtend model.AppSubscriptionTrialExtendPayload `json:"appSubscriptionTrialExtend"`
}

var appSubscriptionTrialExtend = `
mutation AppSubscriptionTrialExtend($id: ID!, $days: Int!) {
	appSubscriptionTrialExtend(id: $id, days: $days) {
		userErrors {
			field
			message
			code
		}
		appSubscription {
			id
			status
			name
			test
			trialDays
			createdAt
			currentPeriodEnd
		}
	}
}
`

func (instance *BillingServiceOp) AppSubscriptionTrialExtend(ctx context.Context, appSubscriptionID string, days int) (*model.AppSubscriptionTrialExtendPayload, error) {
	m := MutationAppSubscriptionTrialExtend{}
	vars := map[string]any{
		"id":   appSubscriptionID,
		"days": days,
	}
	err := instance.client.gql.MutateString(ctx, appSubscriptionTrialExtend, vars, &m)
	if err != nil {
		return nil, err
	}

	if len(m.AppSubscriptionTrialExtend.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.AppSubscriptionTrialExtend.UserErrors)
	}

	return &m.AppSubscriptionTrialExtend, nil
}
