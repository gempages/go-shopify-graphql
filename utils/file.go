package utils

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"time"

	"github.com/gempages/go-helper/tracing"
	"github.com/getsentry/sentry-go"

	pkghttp "github.com/gempages/go-shopify-graphql/http"
)

func DownloadFile(ctx context.Context, file *os.File, url string) error {
	var err error

	span := sentry.StartSpan(ctx, "shopify.download_file")
	span.Description = url
	defer func() {
		tracing.FinishSpan(span, err)
	}()
	ctx = span.Context()

	resp, err := httpGetWithRetry(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(file, resp.Body)
	return err
}

func httpGetWithRetry(url string) (resp *http.Response, err error) {
	var uerr *neturl.Error
	for i := 1; i <= 3; i++ {
		resp, err = http.Get(url)
		if err != nil {
			err = fmt.Errorf("attempt %v: %w", i, err)
			if errors.As(err, &uerr) && (uerr.Timeout() || uerr.Temporary()) || pkghttp.IsConnectionError(err) {
				time.Sleep(time.Duration(i) * time.Second)
				continue
			}
		}
		return
	}
	return
}
