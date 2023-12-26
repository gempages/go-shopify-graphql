package shopify

type (
	QueryOption  func(builder QueryBuilder)
	QueryBuilder interface {
		SetFields(fields string)
		SetQuery(query string)
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
