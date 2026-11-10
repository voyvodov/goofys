export CGO_ENABLED=0

TOOLS_DIR ?= $(CURDIR)/.tools
# Keep the fallback version aligned with .github/workflows/goreleaser.yml.
GORELEASER_VERSION ?= 2.14.3
GORELEASER_LOCAL := $(TOOLS_DIR)/goreleaser-v$(GORELEASER_VERSION)/goreleaser
GORELEASER ?= $(or $(shell command -v goreleaser 2>/dev/null),$(GORELEASER_LOCAL))
STATICCHECK := $(TOOLS_DIR)/staticcheck

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

# Build local release artifacts without publishing or requiring SBOM tooling.
.PHONY: release release-check
release: release-check
	"$(GORELEASER)" release --snapshot --clean --skip sbom

release-check: $(GORELEASER) check
	"$(GORELEASER)" check
	go test ./... -run '^$$'

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
staticcheck: $(STATICCHECK)
	@"$(STATICCHECK)" -checks 'all,-ST1000,-U1000,-ST1020,-ST1001,-SA1019' ./...

.PHONY: unparam
unparam:
	@unparam ./...

.PHONY: check-fmt
check-fmt:
	@set -eu; \
	dirs=$$(go list -f '{{.Dir}}' ./...); \
	unformatted=$$(printf '%s\n' "$$dirs" | while IFS= read -r dir; do gofmt -l "$$dir"/*.go; done); \
	if [ -n "$$unformatted" ]; then \
		printf '%s\n' "Go code is not formatted:" "$$unformatted"; \
		exit 1; \
	fi

.PHONY: check-gomod
check-gomod: ## Check go.mod file
	@go mod tidy -diff

$(GORELEASER_LOCAL):
	mkdir -p "$(@D)"
	GOBIN="$(abspath $(@D))" go install github.com/goreleaser/goreleaser/v2@v$(GORELEASER_VERSION)

$(STATICCHECK): go.mod go.sum
	mkdir -p "$(@D)"
	GOBIN="$(abspath $(@D))" go install honnef.co/go/tools/cmd/staticcheck
