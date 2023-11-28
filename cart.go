package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-shopify-graphql-model/graph/model"
	"github.com/gempages/go-shopify-graphql/graphql"
)

type CartService interface {
	Get(ctx context.Context, id graphql.String) (*Cart, error)
	Create(ctx context.Context, cartInput *CartInput) (graphql.String, error)
	CartLinesUpdate(ctx context.Context, id graphql.ID, cartLinesUpdateInput []CartLineUpdateInput) error
	CartLinesAdd(ctx context.Context, id graphql.ID, lines []CartLineInput) error
	CartLinesRemove(ctx context.Context, id graphql.ID, lineIds []graphql.ID) error
	CartNoteUpdate(ctx context.Context, id graphql.ID, note graphql.String) error
	CartDiscountCodesUpdate(ctx context.Context, id graphql.ID, discountCodes []graphql.String) error
}

type CartServiceOp struct {
	client *Client
}

const cartBaseQuery = `
	id
	attributes {
        key
        value
    }
    buyerIdentity{
        countryCode
        customer {
            addresses(first:250){
                edges {
                    node {
                        address1
                        address2
                    }
                }
            }
        }
    }
    checkoutUrl
	createdAt
    updatedAt
    discountCodes {
        applicable
        code
    }
    estimatedCost {
        totalAmount {
            amount
            currencyCode
        }
        subtotalAmount {
            amount
            currencyCode
        }
        totalTaxAmount {
            amount
            currencyCode
        }
        totalDutyAmount {
            amount
            currencyCode
        }
    }
    lines(first:250) {
        edges {
            node {
                attributes {
                    key
                    value
                }
                id
                quantity
                discountAllocations {
                    discountedAmount {
                        amount
                        currencyCode
                    }
                }
                estimatedCost {
                    subtotalAmount {
                        amount
                        currencyCode
                    }
                    totalAmount {
                        amount 
                        currencyCode
                    }
                }
                merchandise {
                    ... on ProductVariant {
                        id
                    }
                }
                sellingPlanAllocation {
                    priceAdjustments {
                        compareAtPrice {
                            amount
                            currencyCode
                        }
                        perDeliveryPrice {
                            amount
                            currencyCode
                        }
                        price {
                            amount
                            currencyCode
                        }
                        unitPrice {
                            amount  
                            currencyCode
                        }
                    }
                }
            }
        }
    }
    note
`

func (c CartServiceOp) Get(ctx context.Context, id graphql.String) (*Cart, error) {
	q := fmt.Sprintf(`
		query cart($id: ID!) {
			cart(id: $id){
				... on Cart {
					%s
				}
			}
		}
	`, cartBaseQuery)

	vars := map[string]interface{}{
		"id": id,
	}

	out := struct {
		Cart *Cart `json:"cart"`
	}{}
	err := c.client.gql.QueryString(ctx, q, vars, &out)
	if err != nil {
		return nil, err
	}

	return out.Cart, nil
}

type CartResult struct {
	Cart struct {
		ID graphql.String `json:"id,omitempty"`
	}
	UserErrors []UserErrors `json:"userErrors"`
}

type MutationCartCreate struct {
	CartResult CartResult `graphql:"cartCreate(input: $cartInput)" json:"cartCreate"`
}

func (c CartServiceOp) Create(ctx context.Context, cartInput *CartInput) (graphql.String, error) {
	m := MutationCartCreate{}

	vars := map[string]interface{}{
		"cartInput": cartInput,
	}
	err := c.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return "", err
	}

	if len(m.CartResult.UserErrors) > 0 {
		return "", fmt.Errorf("%+v", m.CartResult.UserErrors)
	}
	id := m.CartResult.Cart.ID
	return id, nil
}

type CartLineUpdateInput struct {
	Attributes    []Attribute    `json:"attributes,omitempty"`
	ID            graphql.String `json:"id,omitempty"`
	MerchandiseId graphql.String `json:"merchandiseId,omitempty"`
	Quantity      graphql.Int    `json:"quantity,omitempty"`
	SellingPlanId graphql.String `json:"sellingPlanId,omitempty"`
}

type mutationCartLinesUpdate struct {
	CartLinesUpdateResult CartResult `graphql:"cartLinesUpdate(cartId: $cartId, lines: $lines)" json:"cartLinesUpdate"`
}

func (c CartServiceOp) CartLinesUpdate(ctx context.Context, id graphql.ID, cartLinesUpdateInput []CartLineUpdateInput) error {
	m := mutationCartLinesUpdate{}

	vars := map[string]interface{}{
		"cartId": id,
		"lines":  cartLinesUpdateInput,
	}
	err := c.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return err
	}

	if len(m.CartLinesUpdateResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.CartLinesUpdateResult.UserErrors)
	}

	return nil
}

type mutationCartLinesAdd struct {
	CartLinesAddResult CartResult `graphql:"cartLinesAdd(cartId: $cartId, lines: $lines)" json:"cartLinesAdd"`
}

func (c CartServiceOp) CartLinesAdd(ctx context.Context, id graphql.ID, lines []CartLineInput) error {
	m := mutationCartLinesAdd{}

	vars := map[string]interface{}{
		"cartId": id,
		"lines":  lines,
	}
	err := c.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return err
	}

	if len(m.CartLinesAddResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.CartLinesAddResult.UserErrors)
	}

	return nil
}

type mutationCartLinesRemove struct {
	CartLinesRemoveResult CartResult `graphql:"cartLinesRemove(cartId: $cartId, lineIds: $lineIds)" json:"cartLinesRemove"`
}

func (c CartServiceOp) CartLinesRemove(ctx context.Context, id graphql.ID, lineIds []graphql.ID) error {
	m := mutationCartLinesRemove{}

	vars := map[string]interface{}{
		"cartId":  id,
		"lineIds": lineIds,
	}
	err := c.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return err
	}

	if len(m.CartLinesRemoveResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.CartLinesRemoveResult.UserErrors)
	}
	return nil
}

type mutationCartNoteUpdate struct {
	CartNoteUpdateResult CartResult `graphql:"cartNoteUpdate(cartId: $cartId, note: $note)" json:"cartNoteUpdate"`
}

func (c CartServiceOp) CartNoteUpdate(ctx context.Context, id graphql.ID, note graphql.String) error {
	m := mutationCartNoteUpdate{}

	vars := map[string]interface{}{
		"cartId": id,
		"note":   note,
	}
	err := c.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return err
	}

	if len(m.CartNoteUpdateResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.CartNoteUpdateResult.UserErrors)
	}
	return nil
}

type mutationCartDiscountCodesUpdate struct {
	CartDiscountCodesUpdateResult CartResult `graphql:"cartDiscountCodesUpdate(cartId: $cartId, discountCodes: $discountCodes)" json:"cartDiscountCodesUpdate"`
}

func (c CartServiceOp) CartDiscountCodesUpdate(ctx context.Context, id graphql.ID, discountCodes []graphql.String) error {
	m := mutationCartDiscountCodesUpdate{}

	vars := map[string]interface{}{
		"cartId":        id,
		"discountCodes": discountCodes,
	}
	err := c.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return err
	}

	if len(m.CartDiscountCodesUpdateResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.CartDiscountCodesUpdateResult.UserErrors)
	}
	return nil
}

type Cart struct {
	Attributes    []Attribute        `json:"attributes,omitempty"`
	BuyerIdentity CartBuyerIdentity  `json:"buyerIdentity,omitempty"`
	CheckoutUrl   graphql.String     `json:"checkoutUrl,omitempty"`
	CreatedAt     DateTime           `json:"createdAt,omitempty"`
	DiscountCodes []CartDiscountCode `json:"discountCodes,omitempty"`
	EstimatedCost CartEstimatedCost  `json:"estimatedCost,omitempty"`
	ID            graphql.String     `json:"id,omitempty"`
	Lines         struct {
		Edges []struct {
			Node   CartLine       `json:"node,omitempty"`
			Cursor graphql.String `json:"cursor,omitempty"`
		} `json:"edges,omitempty"`
		PageInfo PageInfo `json:"pageInfo,omitempty"`
	} `json:"lines,omitempty"`
	Note      graphql.String `json:"note,omitempty"`
	UpdatedAt DateTime       `json:"updatedAt,omitempty"`
}

type CartLine struct {
	Attributes            []Attribute              `json:"attributes,omitempty"`
	DiscountAllocations   []CartDiscountAllocation `json:"discountAllocations,omitempty"`
	EstimatedCost         CartLineEstimatedCost    `json:"estimatedCost,omitempty"`
	ID                    graphql.String           `json:"id,omitempty"`
	Merchandise           model.ProductVariant     `json:"merchandise,omitempty"`
	Quantity              graphql.Int              `json:"quantity,omitempty"`
	SellingPlanAllocation SellingPlanAllocation    `json:"sellingPlanAllocation,omitempty"`
}

type CartDiscountAllocation struct {
	DiscountedAmount MoneyV2 `json:"discountedAmount,omitempty"`
}

type CartLineEstimatedCost struct {
	SubtotalAmount MoneyV2 `json:"subtotalAmount,omitempty"`
	TotalAmount    MoneyV2 `json:"totalAmount,omitempty"`
}

type SellingPlanAllocation struct {
	PriceAdjustments []SellingPlanAllocationPriceAdjustment `json:"priceAdjustments,omitempty"`
	SellingPlan      SellingPlan                            `json:"sellingPlan,omitempty"`
}

type SellingPlanAllocationPriceAdjustment struct {
	CompareAtPrice   MoneyV2 `json:"compareAtPrice,omitempty"`
	PerDeliveryPrice MoneyV2 `json:"perDeliveryPrice,omitempty"`
	Price            MoneyV2 `json:"price,omitempty"`
	UnitPrice        MoneyV2 `json:"unitPrice,omitempty"`
}

type SellingPlan struct {
	Description         graphql.String               `json:"description,omitempty"`
	ID                  graphql.String               `json:"id,omitempty"`
	Name                graphql.String               `json:"name,omitempty"`
	Options             []SellingPlanOption          `json:"options,omitempty"`
	PriceAdjustments    []SellingPlanPriceAdjustment `json:"priceAdjustments,omitempty"`
	RecurringDeliveries graphql.Boolean              `json:"recurringDeliveries,omitempty"`
}

type SellingPlanOption struct {
	Name  graphql.String `json:"name,omitempty"`
	Value graphql.String `json:"value,omitempty"`
}

type SellingPlanPriceAdjustment struct {
	// adjustmentValue
	OrderCount graphql.Int `json:"orderCount,omitempty"`
}

type CartDiscountCode struct {
	Applicable graphql.Boolean `json:"applicable,omitempty"`
	Code       graphql.String  `json:"code,omitempty"`
}

type CartEstimatedCost struct {
	SubtotalAmount  MoneyV2 `json:"subtotalAmount,omitempty"`
	TotalAmount     MoneyV2 `json:"totalAmount,omitempty"`
	TotalDutyAmount MoneyV2 `json:"totalDutyAmount,omitempty"`
	TotalTaxAmount  MoneyV2 `json:"totalTaxAmount,omitempty"`
}

type Attribute struct {
	Key   graphql.String `json:"key,omitempty"`
	Value graphql.String `json:"value,omitempty"`
}

type CartBuyerIdentity struct {
	CountryCode CountryCode    `json:"countryCode,omitempty"`
	Customer    CartCustomer   `json:"customer,omitempty"`
	Email       graphql.String `json:"email,omitempty"`
	Phone       graphql.String `json:"phone,omitempty"`
}

type CartInput struct {
	Attributes    []Attribute            `json:"attributes,omitempty"`
	BuyerIdentity CartBuyerIdentityInput `json:"buyerIdentity,omitempty"`
	DiscountCodes []graphql.String       `json:"discountCodes,omitempty"`
	Lines         []CartLineInput        `json:"lines,omitempty"`
	Note          graphql.String         `json:"note,omitempty"`
}

type CartBuyerIdentityInput struct {
	CountryCode         CountryCode    `json:"countryCode,omitempty"`
	CustomerAccessToken graphql.String `json:"customerAccessToken,omitempty"`
	Email               graphql.String `json:"email,omitempty"`
	Phone               graphql.String `json:"phone,omitempty"`
}

type CartLineInput struct {
	Attributes    []Attribute    `json:"attributes,omitempty"`
	MerchandiseId graphql.String `json:"merchandiseId,omitempty"`
	Quantity      graphql.Int    `json:"quantity,omitempty"`
	SellingPlanId graphql.String `json:"sellingPlanId,omitempty"`
}

type CartCustomer struct {
	AcceptsMarketing graphql.Boolean `json:"acceptsMarketing"`
	Addresses        struct {
		Edges []struct {
			Node   MailingAddress `json:"node,omitempty"`
			Cursor graphql.String `json:"cursor,omitempty"`
		} `json:"edges,omitempty"`
		PageInfo PageInfo `json:"pageInfo,omitempty"`
	} `json:"addresses,omitempty"`
	CreatedAt              DateTime       `json:"createdAt,omitempty"`
	DefaultAddress         MailingAddress `json:"defaultAddress,omitempty"`
	DisplayName            graphql.String `json:"displayName,omitempty"`
	Email                  graphql.String `json:"email,omitempty"`
	FirstName              graphql.String `json:"firstName,omitempty"`
	ID                     graphql.String `json:"id,omitempty"`
	LastIncompleteCheckout Checkout       `json:"lastIncompleteCheckout,omitempty"`
	LastName               graphql.String `json:"lastName,omitempty"`
	Orders                 struct {
		Edges []struct {
			Node   Order          `json:"node,omitempty"`
			Cursor graphql.String `json:"cursor,omitempty"`
		} `json:"edges,omitempty"`
		PageInfo PageInfo `json:"pageInfo,omitempty"`
	} `json:"orders,omitempty"`
	Phone     graphql.String   `json:"phone,omitempty"`
	Tags      []graphql.String `json:"tags,omitempty"`
	UpdatedAt DateTime         `json:"updatedAt,omitempty"`
}

type Checkout struct {
	AppliedGiftCards            []AppliedGiftCard      `json:"appliedGiftCards,omitempty"`
	AvailableShippingRates      AvailableShippingRates `json:"availableShippingRates,omitempty"`
	BuyerIdentity               CheckoutBuyerIdentity  `json:"buyerIdentity,omitempty"`
	CompletedAt                 DateTime               `json:"completedAt,omitempty"`
	CreatedAt                   DateTime               `json:"createdAt,omitempty"`
	CurrencyCode                CurrencyCode           `json:"currencyCode,omitempty"`
	CustomAttributes            []Attribute            `json:"customAttributes,omitempty"`
	Email                       graphql.String         `json:"email,omitempty"`
	ID                          graphql.String         `json:"id,omitempty"`
	LineItemsSubtotalPrice      MoneyV2                `json:"lineItemsSubtotalPrice,omitempty"`
	Note                        graphql.String         `json:"note,omitempty"`
	Order                       Order                  `json:"order,omitempty"`
	OrderStatusUrl              graphql.String         `json:"orderStatusUrl,omitempty"`
	PaymentDueV2                MoneyV2                `json:"paymentDueV2,omitempty"`
	Ready                       graphql.Boolean        `json:"ready,omitempty"`
	RequiresShipping            graphql.Boolean        `json:"requiresShipping,omitempty"`
	ShippingAddress             MailingAddress         `json:"shippingAddress,omitempty"`
	ShippingDiscountAllocations []DiscountAllocation   `json:"shippingDiscountAllocations,omitempty"`
	ShippingLine                ShippingRate           `json:"shippingLine,omitempty"`
	SubtotalPriceV2             MoneyV2                `json:"subtotalPriceV2,omitempty"`
	TaxExempt                   graphql.Boolean        `json:"taxExempt,omitempty"`
	TaxesIncluded               graphql.Boolean        `json:"taxesIncluded,omitempty"`
	TotalDuties                 MoneyV2                `json:"totalDuties,omitempty"`
	TotalPriceV2                MoneyV2                `json:"totalPriceV2,omitempty"`
	TotalTaxV2                  MoneyV2                `json:"totalTaxV2,omitempty"`
	UpdatedAt                   DateTime               `json:"updatedAt,omitempty"`
	WebUrl                      graphql.String         `json:"webUrl,omitempty"`
}

type DiscountAllocation struct {
}

type AppliedGiftCard struct {
	AmountUsedV2          MoneyV2        `json:"amountUsedV2,omitempty"`
	BalanceV2             MoneyV2        `json:"balanceV2,omitempty"`
	ID                    graphql.String `json:"id,omitempty"`
	LastCharacters        graphql.String `json:"lastCharacters,omitempty"`
	PresentmentAmountUsed MoneyV2        `json:"presentmentAmountUsed,omitempty"`
}

type AvailableShippingRates struct {
	Ready         graphql.Boolean `json:"ready,omitempty"`
	ShippingRates []ShippingRate  `json:"shippingRates,omitempty"`
}

type ShippingRate struct {
	Handle  graphql.String `json:"handle,omitempty"`
	PriceV2 MoneyV2        `json:"priceV2,omitempty"`
	Title   graphql.String `json:"title,omitempty"`
}

type CheckoutBuyerIdentity struct {
	CountryCode CountryCode `json:"countryCode,omitempty"`
}
