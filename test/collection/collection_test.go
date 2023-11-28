package collection_test

import (
	"fmt"
	"os"
	"strings"

	"github.com/gempages/go-helper/errors"
	"github.com/gempages/go-shopify-graphql"
	shopifyGraph "github.com/gempages/go-shopify-graphql/graph"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	TotalCollectionCount        = 3
	TestSingleQueryCollectionID = "gid://shopify/Collection/453231870266"
	TestCollectionProductCount  = 29
)

var _ = Describe("CollectionService", func() {
	var (
		shopifyClient *shopify.Client
		domain        string
		token         string
	)

	BeforeEach(func() {
		domain = os.Getenv("SHOPIFY_SHOP_DOMAIN")
		token = os.Getenv("SHOPIFY_API_TOKEN")
		opts := []shopifyGraph.Option{
			shopifyGraph.WithToken(token),
		}
		shopifyClient = shopify.NewClientWithOpts(domain, opts...)
	})

	Describe("ListAll", func() {
		It("returns all collections", func() {
			results, err := shopifyClient.Collection.ListAll()
			Expect(err).NotTo(HaveOccurred())
			Expect(results).NotTo(BeEmpty())
			Expect(len(results)).To(Equal(TotalCollectionCount))
			for i := range results {
				Expect(results[i].ID).NotTo(BeEmpty())
				Expect(results[i].Title).NotTo(BeEmpty())
				Expect(results[i].Handle).NotTo(BeEmpty())
				Expect(results[i].Products).NotTo(BeNil())
			}
		})
	})

	Describe("List", func() {
		When("no query is provided", func() {
			It("returns all collections", func() {
				results, err := shopifyClient.Collection.List("")
				Expect(err).NotTo(HaveOccurred())
				Expect(results).NotTo(BeEmpty())
				Expect(len(results)).To(Equal(TotalCollectionCount))
				for i := range results {
					Expect(results[i].ID).NotTo(BeEmpty())
					Expect(results[i].Title).NotTo(BeEmpty())
					Expect(results[i].Handle).NotTo(BeEmpty())
					Expect(results[i].Products).NotTo(BeNil())
				}
			})
		})

		When("id query is provided", func() {
			It("returns collections with correct IDs", func() {
				ids := []string{"453231870266", "453231673658"}
				query := fmt.Sprintf("id:%s", strings.Join(ids, " OR "))
				results, err := shopifyClient.Collection.List(query)
				Expect(err).NotTo(HaveOccurred())
				Expect(results).NotTo(BeEmpty())
				Expect(len(results)).To(Equal(len(ids)))
				for i := range results {
					Expect(results[i].ID).NotTo(BeEmpty())
					id := strings.ReplaceAll(results[i].ID, "gid://shopify/Collection/", "")
					Expect(id).To(BeElementOf(ids))
					Expect(results[i].Title).NotTo(BeEmpty())
					Expect(results[i].Handle).NotTo(BeEmpty())
					Expect(results[i].Products).NotTo(BeNil())
				}
			})
		})
	})

	Describe("ListWithFields", func() {
		It("returns only requested fields", func() {
			fields := `id title handle`
			firstLimit := 1
			results, err := shopifyClient.Collection.ListWithFields(firstLimit, "", "", fields)
			Expect(err).NotTo(HaveOccurred())
			Expect(results).NotTo(BeNil())
			for _, e := range results.Edges {
				Expect(e.Node.ID).NotTo(BeEmpty())
				Expect(e.Node.Title).NotTo(BeEmpty())
				Expect(e.Node.Handle).NotTo(BeEmpty())
				// Other fields should be empty
				Expect(e.Node.Products).To(BeNil())
				Expect(e.Node.TemplateSuffix).To(BeNil())
			}
		})

		When("query first 2 collections", func() {
			It("returns 2 collections", func() {
				fields := `id`
				firstLimit := 2
				results, err := shopifyClient.Collection.ListWithFields(firstLimit, "", "", fields)
				Expect(err).NotTo(HaveOccurred())
				Expect(results).NotTo(BeNil())
				Expect(len(results.Edges)).To(Equal(firstLimit))
			})
		})
	})

	Describe("Get", func() {
		When("ID does not exist", func() {
			It("returns not found error", func() {
				var notExistErr *errors.NotExistsError
				collection, err := shopifyClient.Collection.Get("gid://shopify/Collection/0000")
				Expect(err).To(BeAssignableToTypeOf(notExistErr))
				Expect(collection).To(BeNil())
			})
		})

		When("ID exists", func() {
			It("returns the correct product with all of its variants", func() {
				product, err := shopifyClient.Collection.Get(TestSingleQueryCollectionID)
				Expect(err).NotTo(HaveOccurred())
				Expect(product).NotTo(BeNil())
				Expect(product.ID).To(Equal(TestSingleQueryCollectionID))
				Expect(len(product.Products.Edges)).To(Equal(TestCollectionProductCount))
			})
		})
	})
})
