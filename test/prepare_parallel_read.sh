#!/bin/bash
set -euo pipefail

: "${TEST_ENDPOINT_URL:=http://127.0.0.1:9000}"
export AWS_ACCESS_KEY_ID="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
export AWS_SECRET_ACCESS_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
export AWS_DEFAULT_REGION=us-east-1
command -v aws >/dev/null || { echo "AWS CLI is required" >&2; exit 1; }
curl --fail --silent --show-error "${TEST_ENDPOINT_URL%/}/health/ready" >/dev/null
aws --endpoint-url "$TEST_ENDPOINT_URL" s3api head-bucket --bucket test 2>/dev/null ||
    aws --endpoint-url "$TEST_ENDPOINT_URL" s3api create-bucket --bucket test

mkdir -p /tmp/goofys-parallel-read
for i in f1 f2 f3 f4; do
    of=/tmp/goofys-parallel-read/$i
    [ -f $of ] || dd if=/dev/urandom of=$of bs=1M count=1024
done
of=/tmp/goofys-parallel-read/small
[ -f $of ] || dd if=/dev/urandom of=$of bs=1 count=1234513
aws --endpoint-url "$TEST_ENDPOINT_URL" s3 sync /tmp/goofys-parallel-read/ s3://test/
