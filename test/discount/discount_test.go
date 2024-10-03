package discount_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gempages/go-shopify-graphql-model/graph/model"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gempages/go-shopify-graphql"
	shopifyGraph "github.com/gempages/go-shopify-graphql/graph"
)

var _ = Describe("DiscountService", func() {
	var (
		ctx                context.Context
		shopifyClient      *shopify.Client
		domain             string
		token              string
		shopifyFunctionID  string
		discountIDToDelete string
	)

	BeforeEach(func() {
		ctx = context.Background()
		domain = os.Getenv("SHOPIFY_SHOP_DOMAIN")
		token = os.Getenv("SHOPIFY_API_TOKEN")
		// Follow the step below to get function ID:
		// - Go to Shopify Partners/Apps/All apps in https://partners.shopify.com/
		// - Select an app, go to "Extensions"
		// - Select a Shopify function in the list, you can find its ID in "Function details"
		shopifyFunctionID = os.Getenv("SHOPIFY_DISCOUNT_FUNCTION_ID")
		opts := []shopifyGraph.Option{
			shopifyGraph.WithToken(token),
		}
		shopifyClient = shopify.NewClientWithOpts(domain, opts...)
	})

	AfterEach(func() {
		if discountIDToDelete != "" {
			err := shopifyClient.Discount.AutomaticDelete(ctx, discountIDToDelete)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	Describe("AutomaticAppCreate", func() {
		When("input has nil endsAt", func() {
			It("creates an active discount", func() {
				result, err := shopifyClient.Discount.AutomaticAppCreate(ctx, model.DiscountAutomaticAppInput{
					Title:      aws.String(fmt.Sprintf("GemPages - Test Discount %d", rand.Int())),
					FunctionID: &shopifyFunctionID,
					CombinesWith: &model.DiscountCombinesWithInput{
						ProductDiscounts: aws.Bool(true),
					},
					StartsAt: aws.Time(time.Now()),
					EndsAt:   nil,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(result).NotTo(BeNil())
				Expect(result.DiscountID).NotTo(BeEmpty())
				Expect(result.Title).NotTo(BeEmpty())
				Expect(result.Status.String()).To(Equal("ACTIVE"))
				Expect(result.EndsAt).To(BeNil())

				discountIDToDelete = result.DiscountID
			})
		})

		When("input has not nil endsAt", func() {
			It("creates an active discount", func() {
				result, err := shopifyClient.Discount.AutomaticAppCreate(ctx, model.DiscountAutomaticAppInput{
					Title:      aws.String(fmt.Sprintf("GemPages - Test Discount %d", rand.Int())),
					FunctionID: &shopifyFunctionID,
					CombinesWith: &model.DiscountCombinesWithInput{
						ProductDiscounts: aws.Bool(true),
					},
					StartsAt: aws.Time(time.Now()),
					EndsAt:   aws.Time(time.Now().Add(time.Hour)), // Discount turns EXPIRED after 1 second
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(result).NotTo(BeNil())
				Expect(result.DiscountID).NotTo(BeEmpty())
				Expect(result.Title).NotTo(BeEmpty())
				Expect(result.Status.String()).To(Equal("ACTIVE"))
				Expect(result.EndsAt).NotTo(BeNil())

				discountIDToDelete = result.DiscountID
			})
		})
	})

	Describe("AutomaticAppUpdate", func() {
		When("updates endsAt to time in the past", func() {
			It("marks discount as expired", func() {
				discountCreated, err := shopifyClient.Discount.AutomaticAppCreate(ctx, model.DiscountAutomaticAppInput{
					Title:      aws.String(fmt.Sprintf("GemPages - Test Discount %d", rand.Int())),
					FunctionID: &shopifyFunctionID,
					CombinesWith: &model.DiscountCombinesWithInput{
						ProductDiscounts: aws.Bool(true),
					},
					StartsAt: aws.Time(time.Now()),
					EndsAt:   nil,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(discountCreated).NotTo(BeNil())
				Expect(discountCreated.DiscountID).NotTo(BeEmpty())

				discountIDToDelete = discountCreated.DiscountID

				// Sleep for a second to avoid startsAt equal endsAt when update discount.
				time.Sleep(time.Second)

				discountUpdated, err := shopifyClient.Discount.AutomaticAppUpdate(ctx, discountCreated.DiscountID, shopify.DiscountAutomaticAppInput{
					DiscountAutomaticAppInput: model.DiscountAutomaticAppInput{
						EndsAt: aws.Time(time.Now().Add(-time.Millisecond)),
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(discountUpdated).NotTo(BeNil())
				Expect(discountUpdated.DiscountID).To(Equal(discountCreated.DiscountID))
				Expect(discountUpdated.EndsAt).NotTo(BeNil())
				Expect(discountUpdated.Status.String()).To(Equal("EXPIRED"))
			})
		})

		When("updates endsAt to nil", func() {
			It("marks discount as active", func() {
				discountCreated, err := shopifyClient.Discount.AutomaticAppCreate(ctx, model.DiscountAutomaticAppInput{
					Title:      aws.String(fmt.Sprintf("GemPages - Test Discount %d", rand.Int())),
					FunctionID: &shopifyFunctionID,
					CombinesWith: &model.DiscountCombinesWithInput{
						ProductDiscounts: aws.Bool(true),
					},
					StartsAt: aws.Time(time.Now()),
					EndsAt:   aws.Time(time.Now().Add(time.Millisecond)),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(discountCreated).NotTo(BeNil())
				Expect(discountCreated.DiscountID).NotTo(BeEmpty())
				Expect(discountCreated.EndsAt).NotTo(BeNil())
				Expect(discountCreated.Status.String()).To(Equal("EXPIRED"))

				discountIDToDelete = discountCreated.DiscountID

				// Activate discount in Shopify
				discountUpdated, err := shopifyClient.Discount.AutomaticAppUpdate(ctx, discountCreated.DiscountID,
					shopify.DiscountAutomaticAppInput{
						ClearEndsAt: true,
					})
				Expect(err).NotTo(HaveOccurred())
				Expect(discountUpdated).NotTo(BeNil())
				Expect(discountUpdated.DiscountID).To(Equal(discountCreated.DiscountID))
				Expect(discountUpdated.EndsAt).To(BeNil())
				Expect(discountUpdated.Status.String()).To(Equal("ACTIVE"))
			})
		})
	})
})
