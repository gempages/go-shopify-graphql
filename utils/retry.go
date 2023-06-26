package utils

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

func IsOperationUrlEmptyError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Operation result URL is empty")
}

func IsInvalidTokenError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Invalid API key or access token")
}

func IsInvalidStorefrontTokenError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "401 Unauthorized body")
}

func IsMaxCostLimitError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "max cost limit")
}

func IsPermissionError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "403 Forbidden")
}

func IsNoHostInRequestError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no Host in request URL")
}

func IsThrottledError(err error) bool {
	return err != nil && err.Error() == "Throttled"
}

func IsConnectionError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "connection reset by peer") || strings.Contains(err.Error(), "broken pipe"))
}

func ExecWithRetries(retryCount int, f func() error) error {
	var (
		retries = 0
		err     error
	)
	for {
		err = f()
		if err != nil {
			if uerr, isURLErr := err.(*url.Error); isURLErr && (uerr.Timeout() || uerr.Temporary()) || IsThrottledError(err) || IsConnectionError(err) {
				retries++
				if retries > retryCount {
					return fmt.Errorf("after %v tries: %w", retries, err)
				}
				time.Sleep(time.Duration(retries) * time.Second)
				continue
			}
			return err
		}
		break
	}
	return nil
}
