package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-shopify-graphql/graphql"
)

type BillingService interface {
	AppCreditCreate(ctx context.Context, input *AppCreditCreateInput) (*AppCreditCreateResult, error)
	AppPurchaseOneTimeCreate(ctx context.Context, input *AppPurchaseOneTimeCreateInput) (*AppPurchaseOneTimeCreateResult, error)
	AppSubscriptionCancel(ctx context.Context, id graphql.ID, prorate graphql.Boolean) (*AppSubscriptionCancelResult, error)
	AppSubscriptionCreate(ctx context.Context, input *AppSubscriptionCreateInput) (*AppSubscriptionCreateResult, error)
	AppSubscriptionTrialExtend(ctx context.Context, input *AppSubscriptionTrailExtendInput) (*AppSubscriptionTrailExtendResult, error)
}

type BillingServiceOp struct {
	client *Client
}

type MoneyInput struct {
	Amount       Decimal      `json:"amount,omitempty"`
	CurrencyCode CurrencyCode `json:"currencyCode,omitempty"`
}

type AppCreditCreateInput struct {
	Amount      MoneyInput      `json:"amount,omitempty"`
	Description graphql.String  `json:"description,omitempty"`
	Test        graphql.Boolean `json:"test,omitempty"`
}

type AppPurchaseOneTimeCreateInput struct {
	Name      graphql.String  `json:"name,omitempty"`
	Price     MoneyInput      `json:"price,omitempty"`
	ReturnUrl graphql.URL     `json:"returnUrl,omitempty"`
	Test      graphql.Boolean `json:"test,omitempty"`
}

type AppSubscriptionCancelInput struct {
	ID      graphql.ID      `json:"id,omitempty"`
	Prorate graphql.Boolean `json:"prorate,omitempty"`
}

type AppSubscriptionLineItemInput struct {
	Plan AppPlanInput `json:"plan,omitempty"`
}

type AppPlanInput struct {
	AppRecurringPricingDetails *AppRecurringPricingInput `json:"appRecurringPricingDetails,omitempty"`
	AppUsagePricingDetails     *AppUsagePricingInput     `json:"appUsagePricingDetails,omitempty"`
}

type AppUsagePricingInput struct {
	CappedAmount MoneyInput     `json:"cappedAmount,omitempty"`
	Terms        graphql.String `json:"terms,omitempty"`
}

type AppRecurringPricingInput struct {
	Discount *AppSubscriptionDiscountInput `json:"discount,omitempty"`
	Interval *graphql.String               `json:"interval,omitempty"`
	Price    MoneyInput                    `json:"price"`
}

type AppPricingInterval struct{}

type AppSubscriptionDiscountInput struct {
	DurationLimitInIntervals graphql.Int                       `json:"durationLimitInIntervals,omitempty"`
	Value                    AppSubscriptionDiscountValueInput `json:"value,omitempty"`
}

type AppSubscriptionDiscountValueInput struct {
	Amount     Decimal       `json:"amount,omitempty"`
	Percentage graphql.Float `json:"percentage,omitempty"`
}

type AppSubscriptionCreateInput struct {
	LineItems           []AppSubscriptionLineItemInput `json:"lineItems,omitempty"`
	Name                graphql.String                 `json:"name,omitempty" `
	ReplacementBehavior graphql.String                 `json:"replacementBehavior,omitempty"`
	ReturnUrl           graphql.URL                    `json:"returnUrl,omitempty"`
	Test                graphql.Boolean                `json:"test,omitempty" `
	TrialDays           graphql.Int                    `json:"trialDays,omitempty" `
}

type AppSubscriptionTrailExtendInput struct {
	ID   graphql.ID  `json:"id,omitempty"`
	Days graphql.Int `json:"days,omitempty" `
}

/************************************************ return structures ************************************************/

type AppSubscription struct {
	CreatedAt        graphql.String  `json:"createdAt,omitempty"`
	CurrentPeriodEnd graphql.String  `json:"currentPeriodEnd,omitempty"`
	ID               graphql.ID      `json:"id,omitempty"`
	Name             graphql.String  `json:"name,omitempty"`
	ReturnUrl        graphql.URL     `json:"returnUrl,omitempty"`
	Status           graphql.String  `json:"status,omitempty"`
	Test             graphql.Boolean `json:"test,omitempty"`
	TrialDays        graphql.Int     `json:"trialDays,omitempty"`
}

type AppCreditCreateResult struct {
	AppCredit struct {
		Amount      MoneyV2         `json:"amount,omitempty"`
		CreatedAt   graphql.String  `json:"createdAt"`
		Description graphql.String  `json:"description,omitempty"`
		ID          graphql.ID      `json:"id,omitempty"`
		Test        graphql.Boolean `json:"test,omitempty"`
	}
	UserErrors []UserErrors `json:"userErrors"`
}

type AppPurchaseOneTimeCreateResult struct {
	AppPurchaseOneTime struct {
		Price     MoneyV2         `json:"price,omitempty"`
		CreatedAt graphql.String  `json:"createdAt"`
		Name      graphql.String  `json:"name,omitempty"`
		ID        graphql.ID      `json:"id,omitempty"`
		Test      graphql.Boolean `json:"test,omitempty"`
		Status    graphql.String  `json:"status,omitempty"`
	}
	ConfirmationUrl graphql.URL  `json:"confirmationUrl,omitempty"`
	UserErrors      []UserErrors `json:"userErrors"`
}

type AppSubscriptionCancelResult struct {
	AppSubscription AppSubscription `json:"appSubscription,omitempty"`
	UserErrors      []UserErrors    `json:"userErrors"`
}

type AppSubscriptionCreateResult struct {
	AppSubscription AppSubscription `json:"appSubscription,omitempty"`
	ConfirmationUrl graphql.URL     `json:"confirmationUrl,omitempty"`
	UserErrors      []UserErrors    `json:"userErrors"`
}

type AppSubscriptionTrailExtendResult struct {
	AppSubscription AppSubscription `json:"appSubscription,omitempty"`
	UserErrors      []UserErrors    `json:"userErrors"`
}

type MutationAppCreditCreate struct {
	AppCreditCreateResult AppCreditCreateResult `graphql:"appCreditCreate(amount: $amount, description: $description, test: $test)" json:"appCreditCreate"`
}

type MutationAppPurchaseOneTimeCreate struct {
	AppPurchaseOneTimeCreateResult AppPurchaseOneTimeCreateResult `graphql:"appPurchaseOneTimeCreate(name: $name, price: $price, returnUrl: $returnUrl, test: $test)" json:"appPurchaseOneTimeCreate"`
}

type MutationAppSubscriptionCancel struct {
	AppSubscriptionCancelResult AppSubscriptionCancelResult `graphql:"appSubscriptionCancel(id: $id, prorate: $prorate)" json:"appSubscriptionCancel"`
}

type MutationAppSubscriptionCreate struct {
	AppSubscriptionCreateResult AppSubscriptionCreateResult `graphql:"appSubscriptionCreate(name: $name, returnUrl: $returnUrl, lineItems: $lineItems, test: $test, trialDays: $trialDays)" json:"appSubscriptionCreate"`
}

type MutationAppSubscriptionTrailExtendCreate struct {
	AppSubscriptionTrailExtendResult AppSubscriptionTrailExtendResult `graphql:"appSubscriptionTrialExtend(days: $days, id: $id)" json:"appSubscriptionTrialExtend"`
}

func (instance *BillingServiceOp) AppCreditCreate(ctx context.Context, input *AppCreditCreateInput) (*AppCreditCreateResult, error) {
	m := MutationAppCreditCreate{}

	if input != nil {
		vars := map[string]interface{}{
			"amount":      input.Amount,
			"test":        input.Test,
			"description": input.Description,
		}
		err := instance.client.gql.Mutate(ctx, &m, vars)
		if err != nil {
			return nil, err
		}

		if len(m.AppCreditCreateResult.UserErrors) > 0 {
			return nil, fmt.Errorf("%+v", m.AppCreditCreateResult.UserErrors)
		}
	}
	return &m.AppCreditCreateResult, nil
}

func (instance *BillingServiceOp) AppSubscriptionTrialExtend(ctx context.Context, input *AppSubscriptionTrailExtendInput) (*AppSubscriptionTrailExtendResult, error) {
	m := MutationAppSubscriptionTrailExtendCreate{}

	if input != nil {
		vars := map[string]interface{}{
			"days": input.Days,
			"id":   input.ID,
		}
		err := instance.client.gql.Mutate(ctx, &m, vars)
		if err != nil {
			return nil, err
		}

		if len(m.AppSubscriptionTrailExtendResult.UserErrors) > 0 {
			return nil, fmt.Errorf("%+v", m.AppSubscriptionTrailExtendResult.UserErrors)
		}
	}
	return &m.AppSubscriptionTrailExtendResult, nil
}

func (instance *BillingServiceOp) AppPurchaseOneTimeCreate(ctx context.Context, input *AppPurchaseOneTimeCreateInput) (*AppPurchaseOneTimeCreateResult, error) {
	m := MutationAppPurchaseOneTimeCreate{}

	if input != nil {
		vars := map[string]interface{}{
			"name":      input.Name,
			"price":     input.Price,
			"returnUrl": input.ReturnUrl,
			"test":      input.Test,
		}
		err := instance.client.gql.Mutate(ctx, &m, vars)
		if err != nil {
			return nil, err
		}

		if len(m.AppPurchaseOneTimeCreateResult.UserErrors) > 0 {
			return nil, fmt.Errorf("%+v", m.AppPurchaseOneTimeCreateResult.UserErrors)
		}
	}
	return &m.AppPurchaseOneTimeCreateResult, nil
}

func (instance *BillingServiceOp) AppSubscriptionCancel(ctx context.Context, id graphql.ID, prorate graphql.Boolean) (*AppSubscriptionCancelResult, error) {
	m := MutationAppSubscriptionCancel{}

	vars := map[string]interface{}{
		"id":      id,
		"prorate": prorate,
	}
	err := instance.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return nil, err
	}

	if len(m.AppSubscriptionCancelResult.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.AppSubscriptionCancelResult.UserErrors)
	}
	return &m.AppSubscriptionCancelResult, nil
}

func (instance *BillingServiceOp) AppSubscriptionCreate(ctx context.Context, input *AppSubscriptionCreateInput) (*AppSubscriptionCreateResult, error) {
	m := MutationAppSubscriptionCreate{}

	if input != nil {
		vars := map[string]interface{}{
			"lineItems": input.LineItems,
			"name":      input.Name,
			"returnUrl": input.ReturnUrl,
			"test":      input.Test,
			"trialDays": input.TrialDays,
		}
		err := instance.client.gql.Mutate(ctx, &m, vars)
		if err != nil {
			return nil, err
		}

		if len(m.AppSubscriptionCreateResult.UserErrors) > 0 {
			return nil, fmt.Errorf("%+v", m.AppSubscriptionCreateResult.UserErrors)
		}
	}

	return &m.AppSubscriptionCreateResult, nil
}
