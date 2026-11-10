#!/bin/bash

#set -o xtrace
set -o errexit
set -o nounset

: ${CLOUD:="s3"}
: ${PROXY_BIN:=""}
: ${PROXY_PID:=""}
: ${TIMEOUT:="10m"}
: ${MOUNT:="false"}
: ${GOOGLE_APPLICATION_CREDENTIALS:=""}
RUSTFS_CONTAINER=""

export MOUNT

function cleanup {
    if [ -n "$RUSTFS_CONTAINER" ]; then
        docker stop "$RUSTFS_CONTAINER" >/dev/null
    fi
    if [ "$PROXY_PID" != "" ]; then
        kill $PROXY_PID
    fi
}

T=
if [ $# == 1 ]; then
    T="-check.f $1"
elif [ "$MOUNT" != "false" ]; then
    T="-check.f GoofysTest.TestMount$"
fi

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ "$CLOUD" == "s3" ] && [ -z "${AWS+x}" ] && [ -z "${GCS+x}" ]; then
    export RUSTFS=true
    export RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
    export RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
    command -v curl >/dev/null || { echo "curl is required for the RustFS readiness check" >&2; exit 1; }

    if [ -z "${TEST_ENDPOINT_URL:-}" ]; then
        command -v docker >/dev/null || {
            echo "Docker is required, or set TEST_ENDPOINT_URL to an existing RustFS server" >&2
            exit 1
        }
        RUSTFS_CONTAINER=$(docker run --rm --detach \
            -p 127.0.0.1::9000 \
            -e RUSTFS_ACCESS_KEY -e RUSTFS_SECRET_KEY \
            -e RUSTFS_CONSOLE_ENABLE=false \
            "${RUSTFS_IMAGE:-rustfs/rustfs:1.0.4}")
        port=$(docker port "$RUSTFS_CONTAINER" 9000/tcp)
        TEST_ENDPOINT_URL="http://$port"
    fi
    export TEST_ENDPOINT_URL

    ready=false
    for ((attempt=0; attempt<60; attempt++)); do
        if curl --fail --silent --max-time 2 "${TEST_ENDPOINT_URL%/}/health/ready" >/dev/null; then
            ready=true
            break
        fi
        sleep 1
    done
    if [ "$ready" != true ]; then
        echo "RustFS did not become ready at $TEST_ENDPOINT_URL" >&2
        if [ -n "$RUSTFS_CONTAINER" ]; then
            docker logs "$RUSTFS_CONTAINER" >&2
        fi
        exit 1
    fi
elif [ $CLOUD == "azblob" ]; then
    : ${AZURE_STORAGE_ACCOUNT:="devstoreaccount1"}
    : ${AZURE_STORAGE_KEY:="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="}
    : ${ENDPOINT:="http://127.0.0.1:8080/${AZURE_STORAGE_ACCOUNT}/"}

    if [ ${AZURE_STORAGE_ACCOUNT} == "devstoreaccount1" ]; then
	if ! which azurite >/dev/null; then
	    echo "Azurite missing, run:" >&1
	    echo "npm install -g azurite" >&1
	    exit 1
	fi

	rm -Rf /tmp/azblob
	mkdir -p /tmp/azblob
	PROXY_BIN="azurite-blob -l /tmp/azblob --blobPort 8080 -s"
	#PROXY_BIN="azurite-blob -l /tmp/azblob --blobPort 8080 -d /dev/stdout"
    fi

    export AZURE_STORAGE_ACCOUNT
    export AZURE_STORAGE_KEY
    export ENDPOINT
elif [ $CLOUD == "adlv1" ]; then
    TIMEOUT=40m
elif [ $CLOUD == "gcs" ]; then
    if [ "$GOOGLE_APPLICATION_CREDENTIALS" = "" ]; then
      echo "GOOGLE_APPLICATION_CREDENTIALS must be set" 1>&2
      exit 1
    fi
fi

if [ "$PROXY_BIN" != "" ]; then
    stdbuf -oL -eL $PROXY_BIN &
    PROXY_PID=$!
elif [ "$TIMEOUT" == "10m" ] && [ "${RUSTFS:-false}" != true ]; then
    # higher timeout for testing to real cloud
    TIMEOUT=45m
fi

export CLOUD

# run test in `go test` local mode so streaming output works
(cd internal; go test --failfast -v -timeout $TIMEOUT -check.v $T)
exit $?
