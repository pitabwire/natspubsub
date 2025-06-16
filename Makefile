
SERVICE		?= $(shell basename `go list`)
VERSION		?= $(shell git describe --tags --always --dirty --match=v* 2> /dev/null || cat $(PWD)/.version 2> /dev/null || echo v0)
PACKAGE		?= $(shell go list)
PACKAGES	?= $(shell go list ./...)
FILES		?= $(shell find . -type f -name '*.go' -not -path "./vendor/*")



default: help

help:   ## show this help
	@echo 'usage: make [target] ...'
	@echo ''
	@echo 'targets:'
	@egrep '^(.+)\:\ .*##\ (.+)' ${MAKEFILE_LIST} | sed 's/:.*##/#/' | column -t -c 2 -s '#'

clean:  ## go clean
	go clean

fmt:    ## format the go source files
	go fmt ./...

vet:    ## run go vet on the source files
	go vet ./...

doc:    ## generate godocs and start a local documentation webserver on port 8085
	godoc -http=:8085 -index

goimports:
	find . -name '*.go' -not -path './vendor/*' -not -path './.git/*' -exec sed -i '/^import (/,/^)/{/^$$/d}' {} +
	find . -name '*.go' -not -path './vendor/*' -not -path './.git/*' -exec goimports -w {} +

# this command will run all tests in the repo
# INTEGRATION_TEST_SUITE_PATH is used to run specific tests in Golang,
# if it's not specified it will run all tests
tests: ## runs all system tests
	$(ENV_LOCAL_TEST) \
	FILES=$(go list ./...  | grep -v /vendor/);\
	go test ./... -v -run=$(INTEGRATION_TEST_SUITE_PATH)  -coverprofile=coverage.out;\
	RETURNCODE=$$?;\
	if [ "$$RETURNCODE" -ne 0 ]; then\
		echo "unit tests failed with error code: $$RETURNCODE" >&2;\
		exit 1;\
	fi;\
	go tool cover -html=coverage.out -o coverage.html

build: clean fmt vet tests ## run all preliminary steps and tests the setup
