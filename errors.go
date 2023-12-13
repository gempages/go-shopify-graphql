package shopify

import (
	"errors"
	"strings"

	"github.com/gempages/go-shopify-graphql/graphql"
)

func IsInvalidTokenError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Invalid API key or access token")
}

func IsInvalidStorefrontTokenError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "401 Unauthorized")
}

func IsPermissionError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "403 Forbidden")
}

func IsPaymentRequiredError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, graphql.ErrPaymentRequired)
}

func IsLockedError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, graphql.ErrLocked)
}

func IsRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, graphql.ErrMaxCostExceeded)
}

func IsTooManyRequestsError(err error) bool {
	return err != nil &&
		(strings.Contains(err.Error(), "Reduce request rates to resume uninterrupted service") ||
			strings.Contains(err.Error(), "The rate of change to"))
}

func IsUnauthorizedError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, graphql.ErrUnauthorized)
}

func IsForbiddenError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, graphql.ErrForbidden)
}

func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, graphql.ErrNotFound)
}

func IsInternalError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, graphql.ErrInternal)
}
