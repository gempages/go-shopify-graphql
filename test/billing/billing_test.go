package billing_test

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gempages/go-shopify-graphql-model/graph/model"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/shopspring/decimal"

	"github.com/gempages/go-shopify-graphql"
	shopifyGraph "github.com/gempages/go-shopify-graphql/graph"
)

var _ = Describe("BillingService", Serial, func() {
	var (
		ctx           context.Context
		shopifyClient *shopify.Client
		domain        string
		token         string
	)

	BeforeEach(func() {
		ctx = context.Background()
		domain = os.Getenv("SHOPIFY_SHOP_DOMAIN")
		token = os.Getenv("SHOPIFY_API_TOKEN")
		opts := []shopifyGraph.Option{
			shopifyGraph.WithToken(token),
		}
		shopifyClient = shopify.NewClientWithOpts(domain, opts...)
	})

	Describe("AppSubscriptionCreate", func() {
		When("subscription includes app recurring pricing with `ANNUAL` interval + app usage pricing", func() {
			It("returns error", func() {
				name := "Subscription Name"
				returnUrl := "https://return.url/"
				test := true
				interval := model.AppPricingIntervalAnnual
				trialDays := 0
				lineItems := []model.AppSubscriptionLineItemInput{
					{
						Plan: &model.AppPlanInput{
							AppRecurringPricingDetails: &model.AppRecurringPricingInput{
								Interval: &interval,
								Price: &model.MoneyInput{
									Amount:       decimal.New(300, 1),
									CurrencyCode: model.CurrencyCodeUsd,
								},
								Discount: &model.AppSubscriptionDiscountInput{
									Value: &model.AppSubscriptionDiscountValueInput{
										//Amount: &discountAmount,
										Percentage: aws.Float64(0.1),
									},
									DurationLimitInIntervals: aws.Int(3),
								},
							},
						},
					},
					{
						Plan: &model.AppPlanInput{
							AppUsagePricingDetails: &model.AppUsagePricingInput{
								CappedAmount: &model.MoneyInput{
									Amount:       decimal.New(300, 1),
									CurrencyCode: model.CurrencyCodeUsd,
								},
								Terms: "test usage pricing",
							},
						},
					},
				}

				result, err := shopifyClient.Billing.AppSubscriptionCreate(ctx, shopify.AppSubscriptionCreateInput{
					Name:      name,
					ReturnUrl: returnUrl,
					LineItems: lineItems,
					Test:      &test,
					TrialDays: &trialDays,
				})
				Expect(err).To(HaveOccurred())
				Expect(result).To(BeNil())
			})
		})

		When("app recurring pricing discount percentage >= 1", func() {
			It("returns error", func() {
				name := "Subscription Name"
				returnUrl := "https://return.url/"
				test := true
				interval := model.AppPricingIntervalEvery30Days
				trialDays := 0
				lineItems := []model.AppSubscriptionLineItemInput{
					{
						Plan: &model.AppPlanInput{
							AppRecurringPricingDetails: &model.AppRecurringPricingInput{
								Interval: &interval,
								Price: &model.MoneyInput{
									Amount:       decimal.New(300, 0),
									CurrencyCode: model.CurrencyCodeUsd,
								},
								Discount: &model.AppSubscriptionDiscountInput{
									Value: &model.AppSubscriptionDiscountValueInput{
										Percentage: aws.Float64(1.1),
									},
									DurationLimitInIntervals: aws.Int(3),
								},
							},
						},
					},
				}

				result, err := shopifyClient.Billing.AppSubscriptionCreate(ctx, shopify.AppSubscriptionCreateInput{
					Name:      name,
					ReturnUrl: returnUrl,
					LineItems: lineItems,
					Test:      &test,
					TrialDays: &trialDays,
				})
				Expect(err).To(HaveOccurred())
				Expect(result).To(BeNil())
			})
		})

		When("subscription includes app recurring pricing with `EVERY_30_DAYS` interval + app usage pricing", func() {
			It("creates a charge", func() {
				name := "Subscription Name"
				returnUrl := "https://return.url/"
				test := true
				interval := model.AppPricingIntervalEvery30Days
				trialDays := 0
				discountAmount := decimal.New(400, 0)
				lineItems := []model.AppSubscriptionLineItemInput{
					{
						Plan: &model.AppPlanInput{
							AppRecurringPricingDetails: &model.AppRecurringPricingInput{
								Interval: &interval,
								Price: &model.MoneyInput{
									Amount:       decimal.New(300, 1),
									CurrencyCode: model.CurrencyCodeUsd,
								},
								Discount: &model.AppSubscriptionDiscountInput{
									Value: &model.AppSubscriptionDiscountValueInput{
										Amount: &discountAmount,
									},
									DurationLimitInIntervals: aws.Int(3),
								},
							},
						},
					},
					{
						Plan: &model.AppPlanInput{
							AppUsagePricingDetails: &model.AppUsagePricingInput{
								CappedAmount: &model.MoneyInput{
									Amount:       decimal.New(300, 1),
									CurrencyCode: model.CurrencyCodeUsd,
								},
								Terms: "test usage pricing",
							},
						},
					},
				}

				result, err := shopifyClient.Billing.AppSubscriptionCreate(ctx, shopify.AppSubscriptionCreateInput{
					Name:      name,
					ReturnUrl: returnUrl,
					LineItems: lineItems,
					Test:      &test,
					TrialDays: &trialDays,
					//ReplacementBehavior: &standard,
				})

				Expect(err).NotTo(HaveOccurred())
				Expect(result).NotTo(BeNil())
				Expect(result.ConfirmationURL).NotTo(BeNil())
				Expect(*result.ConfirmationURL).NotTo(BeEmpty())
				Expect(result.AppSubscription).NotTo(BeNil())
				Expect(result.AppSubscription.CreatedAt).NotTo(BeNil())
				Expect(result.AppSubscription.GetID()).NotTo(BeEmpty())
				Expect(result.AppSubscription.Name).To(Equal(name))
				Expect(result.AppSubscription.Test).To(Equal(test))
				Expect(result.AppSubscription.ReturnURL).To(Equal(returnUrl))
				Expect(result.AppSubscription.LineItems).To(HaveLen(len(lineItems)))
				Expect(result.AppSubscription.TrialDays).To(Equal(trialDays))
			})
		})

		When("subscription includes app recurring pricing", func() {
			It("creates a charge", func() {
				name := "Subscription Name"
				returnUrl := "https://return.url/"
				test := true
				interval := model.AppPricingIntervalEvery30Days
				trialDays := 0
				replacementBehavior := model.AppSubscriptionReplacementBehaviorApplyImmediately
				lineItems := []model.AppSubscriptionLineItemInput{
					{
						Plan: &model.AppPlanInput{
							AppRecurringPricingDetails: &model.AppRecurringPricingInput{
								Interval: &interval,
								Price: &model.MoneyInput{
									Amount:       decimal.New(300, 1),
									CurrencyCode: model.CurrencyCodeUsd,
								},
								Discount: &model.AppSubscriptionDiscountInput{
									Value: &model.AppSubscriptionDiscountValueInput{
										//Amount: &discountAmount,
										Percentage: aws.Float64(0.1),
									},
									DurationLimitInIntervals: aws.Int(3),
								},
							},
						},
					},
				}

				result, err := shopifyClient.Billing.AppSubscriptionCreate(ctx, shopify.AppSubscriptionCreateInput{
					Name:                name,
					ReturnUrl:           returnUrl,
					LineItems:           lineItems,
					Test:                &test,
					TrialDays:           &trialDays,
					ReplacementBehavior: &replacementBehavior,
				})

				Expect(err).NotTo(HaveOccurred())
				Expect(result).NotTo(BeNil())
				Expect(result.ConfirmationURL).NotTo(BeNil())
				Expect(*result.ConfirmationURL).NotTo(BeEmpty())
				Expect(result.AppSubscription).NotTo(BeNil())
				Expect(result.AppSubscription.CreatedAt).NotTo(BeNil())
				Expect(result.AppSubscription.GetID()).NotTo(BeEmpty())
				Expect(result.AppSubscription.Name).To(Equal(name))
				Expect(result.AppSubscription.Test).To(Equal(test))
				Expect(result.AppSubscription.ReturnURL).To(Equal(returnUrl))
				Expect(result.AppSubscription.LineItems).To(HaveLen(len(lineItems)))
				Expect(result.AppSubscription.TrialDays).To(Equal(trialDays))
			})
		})
	})
})
