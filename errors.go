package shopify

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gempages/go-shopify-graphql-model/graph/model"

	"github.com/gempages/go-shopify-graphql/graphql"
)

type DiscountError struct {
	Code    model.DiscountErrorCode `json:"code"`
	Message string                  `json:"message"`
}

func (m *DiscountError) Error() string {
	return m.Message
}

func NewDiscountError(code model.DiscountErrorCode, message string) error {
	return &DiscountError{Code: code, Message: message}
}

func NewDiscountErrorf(code model.DiscountErrorCode, format string, args ...any) error {
	return &DiscountError{Code: code, Message: fmt.Sprintf(format, args...)}
}

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
	return err != nil && errors.Is(err, graphql.ErrPaymentRequired)
}

func IsLockedError(err error) bool {
	return err != nil && errors.Is(err, graphql.ErrLocked)
}

func IsRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, graphql.ErrMaxCostExceeded) ||
		strings.Contains(err.Error(), "Reduce request rates to resume uninterrupted service") ||
		strings.Contains(err.Error(), "The rate of change to")
}

func IsNotExistError(err error) bool {
	return err != nil && strings.Contains(err.Error(), graphql.ErrNotExisted.Error())
}

// IsValidationDiscountError checks if the error indicates that the active period in discount overlaps with another price rule.
func IsValidationDiscountError(err error) bool {
	var discountErr *DiscountError
	if errors.As(err, &discountErr) {
		return discountErr.Code == model.DiscountErrorCodeInvalid || discountErr.Code == model.DiscountErrorCodeMaxAppDiscounts
	}
	return false
}

func IsFileNotExistError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "FILE_DOES_NOT_EXIST")
}

func IsUnauthorizedError(err error) bool {
	return err != nil && errors.Is(err, graphql.ErrUnauthorized)
}

func IsForbiddenError(err error) bool {
	return err != nil && errors.Is(err, graphql.ErrForbidden)
}

func IsNotFoundError(err error) bool {
	return err != nil && errors.Is(err, graphql.ErrNotFound)
}

func IsInternalError(err error) bool {
	return err != nil && errors.Is(err, graphql.ErrInternal)
}

func IsServiceUnavailable(err error) bool {
	return err != nil && errors.Is(err, graphql.ErrServiceUnavailable)
}

func IsGatewayTimeout(err error) bool {
	return err != nil && errors.Is(err, graphql.ErrGatewayTimeout)
}

func IsAddressTakenError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Address for this topic has already been taken")
}
