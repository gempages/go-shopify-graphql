package product_test

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/gempages/go-helper/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gempages/go-shopify-graphql"
	shopifyGraph "github.com/gempages/go-shopify-graphql/graph"
)

const (
	TotalProductCount        = 29
	TestSingleQueryProductID = "gid://shopify/Product/8427241144634"
	TestProductVariantCount  = 72
	TestProductMediaCount    = 6
)

var _ = Describe("ProductService", func() {
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
			shopifyGraph.WithVersion("2024-10"),
		}
		shopifyClient = shopify.NewClientWithOpts(domain, opts...)
	})

	Describe("List", func() {
		When("no query is provided", func() {
			It("returns all products", func() {
				results, err := shopifyClient.Product.List(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(results).NotTo(BeEmpty())
				Expect(len(results)).To(Equal(TotalProductCount))
				for i := range results {
					Expect(results[i].ID).NotTo(BeEmpty())
					Expect(results[i].Title).NotTo(BeEmpty())
					Expect(results[i].Handle).NotTo(BeEmpty())
					Expect(results[i].Images).NotTo(BeNil())
					Expect(results[i].Media).NotTo(BeNil())
					Expect(results[i].Variants).NotTo(BeNil())
					Expect(results[i].CreatedAt).NotTo(BeZero())
				}
			})
		})

		When("ID query option is provided", func() {
			It("returns products with correct IDs", func() {
				ids := []string{"8427241144634", "8427240423738", "8427239178554"}
				query := fmt.Sprintf("id:%s", strings.Join(ids, " OR "))
				results, err := shopifyClient.Product.List(ctx, shopify.WithQuery(query))
				Expect(err).NotTo(HaveOccurred())
				Expect(results).NotTo(BeEmpty())
				Expect(len(results)).To(Equal(len(ids)))
				for i := range results {
					Expect(results[i].ID).NotTo(BeEmpty())
					id := strings.ReplaceAll(results[i].ID, "gid://shopify/Product/", "")
					Expect(id).To(BeElementOf(ids))
					Expect(results[i].Title).NotTo(BeEmpty())
					Expect(results[i].Handle).NotTo(BeEmpty())
					Expect(results[i].Images).NotTo(BeNil())
					Expect(results[i].Media).NotTo(BeNil())
					Expect(results[i].Variants).NotTo(BeNil())
					Expect(results[i].CreatedAt).NotTo(BeZero())
				}
			})
		})

		When("fields option is provided", func() {
			It("returns only requested fields", func() {
				fields := `id title handle`
				results, err := shopifyClient.Product.List(ctx, shopify.WithFields(fields))
				Expect(err).NotTo(HaveOccurred())
				Expect(results).NotTo(BeEmpty())
				for i := range results {
					Expect(results[i].ID).NotTo(BeEmpty())
					Expect(results[i].Title).NotTo(BeEmpty())
					Expect(results[i].Handle).NotTo(BeEmpty())
					Expect(results[i].Images).To(BeNil())
					Expect(results[i].Media).To(BeNil())
					Expect(results[i].Variants).To(BeNil())
					Expect(results[i].CreatedAt).To(BeZero())
				}
			})
		})
	})

	Describe("ListWithFields", func() {
		It("returns only requested fields", func() {
			fields := `id title handle`
			firstLimit := 1
			results, err := shopifyClient.Product.ListWithFields(ctx, &shopify.ListProductRequest{
				Fields: fields,
				First:  firstLimit,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(results).NotTo(BeNil())
			for _, e := range results.Edges {
				Expect(e.Node.ID).NotTo(BeEmpty())
				Expect(e.Node.Title).NotTo(BeEmpty())
				Expect(e.Node.Handle).NotTo(BeEmpty())
				// Other fields should be empty
				Expect(e.Node.Images).To(BeNil())
				Expect(e.Node.Media).To(BeNil())
				Expect(e.Node.Variants).To(BeNil())
			}
		})

		When("query first 5 products", func() {
			It("returns 5 products", func() {
				fields := `id`
				firstLimit := 5
				results, err := shopifyClient.Product.ListWithFields(ctx, &shopify.ListProductRequest{
					Fields: fields,
					First:  firstLimit,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(results).NotTo(BeNil())
				Expect(len(results.Edges)).To(Equal(firstLimit))
			})
		})

		When("query includes interface type media", func() {
			It("can returns media", func() {
				fields := fmt.Sprintf("id %s", mediaQuery)
				firstLimit := 5
				results, err := shopifyClient.Product.ListWithFields(ctx, &shopify.ListProductRequest{
					Fields: fields,
					First:  firstLimit,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(results).NotTo(BeNil())
				Expect(len(results.Edges)).To(Equal(firstLimit))
				for _, e := range results.Edges {
					Expect(e.Node.Media).NotTo(BeNil())
					for i := range e.Node.Media.Edges {
						Expect(e.Node.Media.Edges[i].Node).NotTo(BeNil())
					}
				}
			})
		})
	})

	Describe("Get", func() {
		When("ID does not exist", func() {
			It("returns not found error", func() {
				var notExistErr *errors.NotExistsError
				product, err := shopifyClient.Product.Get(ctx, "gid://shopify/Product/0000")
				Expect(err).To(BeAssignableToTypeOf(notExistErr))
				Expect(product).To(BeNil())
			})
		})

		When("ID exists", func() {
			It("returns the correct product with all of its variants", func() {
				product, err := shopifyClient.Product.Get(ctx, TestSingleQueryProductID)
				Expect(err).NotTo(HaveOccurred())
				Expect(product).NotTo(BeNil())
				Expect(product.ID).To(Equal(TestSingleQueryProductID))
				Expect(len(product.Variants.Edges)).To(Equal(TestProductVariantCount))
			})
		})
	})

	Describe("GetWithFields", func() {
		When("ID does not exist", func() {
			It("returns not found error", func() {
				var notExistErr *errors.NotExistsError
				product, err := shopifyClient.Product.GetWithFields(ctx, "gid://shopify/Product/0000", "id")
				Expect(err).To(BeAssignableToTypeOf(notExistErr))
				Expect(product).To(BeNil())
			})
		})

		When("query media connection", func() {
			It("returns product with any type of media", func() {
				fields := fmt.Sprintf("id %s", mediaQuery)
				product, err := shopifyClient.Product.GetWithFields(ctx, TestSingleQueryProductID, fields)
				Expect(err).NotTo(HaveOccurred())
				Expect(product).NotTo(BeNil())
				Expect(product.ID).To(Equal(TestSingleQueryProductID))
				Expect(len(product.Media.Edges)).To(Equal(TestProductMediaCount))
				for i := range product.Media.Edges {
					Expect(product.Media.Edges[i].Node).NotTo(BeNil())
				}
			})
		})
	})
})

var mediaQuery = `media(first: 10) {
	edges {
		node {
			__typename
			mediaContentType
			...on MediaImage {
				id
				alt
				mimeType
				image {
					height
					src
					width
				}
			}
			...on Model3d {
				id
				alt
				originalSource {
					url
					format
					filesize
					mimeType
				}
				preview {
					image {
						src
					}
				}
			}
			...on Video {
				id
				alt
				duration
				originalSource {
					url
					format
					mimeType
					height
					width
				}
				preview {
					image {
						src
					}
				}
			}
			...on ExternalVideo {
				id
				originUrl
				embedUrl
				preview {
					image {
						src
					}
				}
			}
		}
		cursor
	}
	pageInfo {
		hasNextPage
	}
}`
