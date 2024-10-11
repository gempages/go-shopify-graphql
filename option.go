package shopify

type (
	QueryOption  func(builder QueryBuilder)
	QueryBuilder interface {
		SetFields(fields string)
		SetQuery(query string)
		SetFirst(first int)
		SetAfter(after string)
	}
)

func WithFields(fields string) QueryOption {
	return func(b QueryBuilder) {
		b.SetFields(fields)
	}
}

func WithQuery(query string) QueryOption {
	return func(b QueryBuilder) {
		b.SetQuery(query)
	}
}

func WithFirst(first int) QueryOption {
	return func(b QueryBuilder) {
		b.SetFirst(first)
	}
}

func WithAfter(after string) QueryOption {
	return func(b QueryBuilder) {
		b.SetAfter(after)
	}
}
