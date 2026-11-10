FROM ubuntu:24.04

ARG GO_VERSION=1.27.3

RUN apt-get update \
    && apt-get install -y ca-certificates curl fuse3 python3 \
    && curl -sSL https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz | \
    tar -C /usr/local -xzf - \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
    
ENV PATH=$PATH:/usr/local/go/bin

