package shopify

type QueryOption func(builder *bulkQueryBuilder)

func WithAfter(after string) QueryOption {
	return func(b *bulkQueryBuilder) {
		b.after = &after
	}
}

func WithFields(fields string) QueryOption {
	return func(b *bulkQueryBuilder) {
		b.fields = fields
	}
}

func WithFirst(first int) QueryOption {
	return func(b *bulkQueryBuilder) {
		b.first = &first
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
