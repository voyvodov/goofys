export CGO_ENABLED=0

VERSION := `git describe --abbrev=0 --tags || echo "0.0.0"`
BUILD := `git rev-parse --short HEAD`
LDFLAGS=-ldflags "-X=github.com/voyvodov/goofys/internal.VersionNumber=$(VERSION) -X=github.com/voyvodov/goofys/internal.VersionHash=$(BUILD)"

.PHONY: test
test: s3proxy.jar
	./test/run-tests.sh

s3proxy.jar:
	wget https://github.com/gaul/s3proxy/releases/download/s3proxy-2.2.0/s3proxy -O s3proxy.jar

get-deps: s3proxy.jar
	go get -t ./...

build:
	go build ${LDFLAGS}

install:
	go install ${LDFLAGS}

##@ Bootstrap
# See following issues for why errors are ignored with `-e` flag:
# 	* https://github.com/golang/go/issues/61857
# 	* https://github.com/golang/go/issues/59186
.PHONY: bootstrap
bootstrap: ## Install tooling
	@go install $$(go list -e -f '{{join .Imports " "}}' ./internal/tools/tools.go)

.PHONY: check
check: staticcheck check-fmt check-gomod

.PHONY: staticcheck
staticcheck:
	@staticcheck -checks 'all,-ST1000,-U1000,-ST1020,-ST1001,-SA1019' ./...

.PHONY: unparam
unparam:
	@unparam ./...

.PHONY: check-fmt
check-fmt:
	@if [ $$(go fmt -mod=mod ./...) ]; then\
		echo "Go code is not formatted";\
		exit 1;\
	fi

.PHONY: check-gomod
check-gomod: ## Check go.mod file
	@go mod tidy
	@git diff --exit-code -- go.sum go.mod