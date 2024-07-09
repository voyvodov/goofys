export CGO_ENABLED=0

export STAGINGVERSION ?= $(shell git describe --long --tags --match='v*' --dirty 2>/dev/null || git rev-list -n1 HEAD)
export BUILD_DATE ?= $(shell date --iso-8601=minutes)
BINDIR ?= bin
LDFLAGS ?= -s -w -X main.version=${STAGINGVERSION} -X main.builddate=${BUILD_DATE} -extldflags '-static'
FUSERMOUNT3PROXY_BINARY = fusermount3-proxy

run-test: s3proxy.jar
	./test/run-tests.sh

s3proxy.jar:
	wget https://github.com/gaul/s3proxy/releases/download/s3proxy-1.8.0/s3proxy -O s3proxy.jar

get-deps: s3proxy.jar
	go get -t ./...

build:
	go build -ldflags "-X main.Version=`git rev-parse HEAD`"

install:
	go install -ldflags "-X main.Version=`git rev-parse HEAD`"

fusermount3-proxy:
	mkdir -p ${BINDIR}
	CGO_ENABLED=0 GOOS=linux GOARCH=$(shell dpkg --print-architecture) go build -ldflags "${LDFLAGS}" -o ${BINDIR}/${FUSERMOUNT3PROXY_BINARY} meta-fuse-csi-plugin/cmd/fusermount3-proxy/main.go

