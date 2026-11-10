FROM golang:1.20.7-alpine AS fusermount3-proxy-builder

# Install required build dependencies
RUN apk update && apk upgrade && apk --no-cache add make gcc g++ libc-dev fuse-dev

WORKDIR /meta-fuse-csi-plugin
ADD ./meta-fuse-csi-plugin .
# Build the fusermount3-proxy
RUN make fusermount3-proxy BINDIR=/bin

FROM golang:1.20.7-alpine AS goofys-builder

# Install required build dependencies
RUN apk update && apk upgrade && apk --no-cache add git make gcc g++ libc-dev fuse-dev

WORKDIR /goofys
ADD . .
# Build the goofys app
RUN make build

# 3.20.3 is the latest as of this commit (September 09 2024)
FROM alpine:3.20.3

# Install necessary runtime dependencies
RUN apk update && apk upgrade && apk --no-cache add ca-certificates bash aws-cli

# Copy the test file
COPY <<EOF /test.txt
This is a test file for RustFS
EOF

# Populate a local RustFS test bucket.
COPY <<'EOF' /configure_rustfs.sh
#!/bin/bash
set -eu
export AWS_ACCESS_KEY_ID="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
export AWS_SECRET_ACCESS_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
export AWS_DEFAULT_REGION=us-east-1
endpoint="${TEST_ENDPOINT_URL:-http://localhost:9000}"
aws --endpoint-url "$endpoint" s3 mb s3://test-bucket
aws --endpoint-url "$endpoint" s3 cp /test.txt s3://test-bucket/
EOF
RUN chmod +x /configure_rustfs.sh

# Get goofys build from the build stage
COPY --from=goofys-builder /goofys/goofys .

# Get fusermount3-proxy from the build stage and set up symlink
COPY --from=fusermount3-proxy-builder /bin/fusermount3-proxy /usr/bin/fusermount3
RUN ln -sf /usr/bin/fusermount3 /bin/fusermount
