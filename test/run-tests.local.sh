#!/bin/bash

#set -o xtrace
set -o errexit
set -o nounset

cleanup () {
  docker compose -p goofys-ci -f test/docker-compose.tests.yml down --volumes
}

cd "$(dirname "$0")/.."
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker compose -p goofys-ci -f test/docker-compose.tests.yml up \
  --build --abort-on-container-exit --exit-code-from tests
