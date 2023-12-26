package shopify

type QueryOption func(builder *bulkQueryBuilder)

func WithFields(fields string) QueryOption {
	return func(b *bulkQueryBuilder) {
		b.fields = fields
	}
}

func WithQuery(query string) QueryOption {
	return func(b *bulkQueryBuilder) {
		b.query = &query
	}
}

func WithReverse(reverse bool) QueryOption {
	return func(b *bulkQueryBuilder) {
		b.reverse = &reverse
	}
}

func WithSavedSearchID(savedSearchID string) QueryOption {
	return func(b *bulkQueryBuilder) {
		b.savedSearchID = &savedSearchID
	}
}

func WithSortKey(sortKey string) QueryOption {
	return func(b *bulkQueryBuilder) {
		b.sortKey = &sortKey
	}
}
