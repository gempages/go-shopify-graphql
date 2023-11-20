package graphql

import (
	"errors"
)

var (
	// ErrLocked means the shop isn’t available. This can happen when stores repeatedly exceed API rate limits or due to fraud risk.
	ErrLocked = errors.New("locked")
	// ErrPaymentRequired means the shop is frozen. The shop owner will need to pay the outstanding balance to unfreeze the shop.
	ErrPaymentRequired = errors.New("payment required")
	// ErrMaxCostExceeded means API rate limit has been reached. The API supports a maximum of 1000 cost points per app per store per minute.
	// This quota replenishes at a rate of 50 cost points per second.
	ErrMaxCostExceeded = errors.New("max cost exceeded")
	ErrUnauthorized    = errors.New("unauthorized")
	ErrForbidden       = errors.New("forbidden")
	ErrNotFound        = errors.New("not found")
	ErrInternal        = errors.New("internal error")
)
