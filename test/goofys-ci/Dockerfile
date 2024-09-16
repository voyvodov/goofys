FROM ubuntu:20.04

ARG GO_VERSION=1.20.7

RUN apt-get update \
    && apt-get install -y curl openjdk-17-jre fuse3 python \
    && curl -sSL https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz | \
    tar -C /usr/local -xzf - \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
    
ENV PATH=$PATH:/usr/local/go/bin

