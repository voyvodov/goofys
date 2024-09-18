FROM golang:1.20.7 as fusermount3-proxy-builder

WORKDIR /meta-fuse-csi-plugin
ADD ./meta-fuse-csi-plugin .
# Builds the meta-fuse-csi-plugin app
RUN make fusermount3-proxy BINDIR=/bin

FROM golang:1.20.7 as goofys-builder

WORKDIR /goofys
ADD . .
# Builds the goofys app
RUN make build

FROM ubuntu:22.04

RUN apt update && apt upgrade -y
RUN apt install -y ca-certificates wget libfuse2 fuse3

# prepare for MinIO
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/bin/mc && chmod +x /usr/bin/mc

COPY <<EOF /test.txt
This is a test file for minio
EOF

COPY <<EOF /configure_minio.sh
#!/bin/bash
set -eux
/usr/bin/mc alias set k8s-minio-dev http://localhost:9000 minioadmin minioadmin
/usr/bin/mc mb k8s-minio-dev/test-bucket
/usr/bin/mc cp /test.txt k8s-minio-dev/test-bucket
EOF
RUN chmod +x /configure_minio.sh

#Get goofys build from first step
COPY --from=goofys-builder /goofys/goofys .

COPY --from=fusermount3-proxy-builder /bin/fusermount3-proxy /bin/fusermount3
RUN ln -sf /bin/fusermount3 /bin/fusermount
