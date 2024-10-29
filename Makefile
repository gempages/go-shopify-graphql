.PHONY: help

TESTDIR?=./...

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


govet:
	go vet ./...

gotest: govet
gotest: ## Run all test cases or specific cases - example: make gotest TESTDIR=./internal/app/usecase/theme_page ARG=--ginkgo.focus=TEST_CASE_PATTERN
	go clean -testcache
	MODE=test go test -cover -race $(TESTDIR) $(ARG)

gotest-mac: govet
gotest-mac: ## Run all test cases or specific cases - example: make gotest TESTDIR=./internal/app/usecase/theme_page ARG=--ginkgo.focus=TEST_CASE_PATTERN
	go clean -testcache
	MODE=test go test -cover -race -ldflags=-extldflags=-Wl,-ld_classic $(TESTDIR) $(ARG)
