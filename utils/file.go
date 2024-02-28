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
	pkghttp "github.com/gempages/go-shopify-graphql/http"
	"github.com/getsentry/sentry-go"
)

func CloseFile(f *os.File) {
	err := f.Close()
	if err != nil {
		panic(err)
	}
}

func ReadFile(file string) (data string, err error) {
	var bytes []byte
	bytes, err = os.ReadFile(file)
	data = string(bytes)
	return
}

func DownloadFile(ctx context.Context, filepath string, url string) error {
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

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer CloseFile(out)

	_, err = io.Copy(out, resp.Body)
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
