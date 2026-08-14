FROM golang:1.24-alpine3.21 AS build

# Build proxy from source. This is a shallow clone of the repository to avoid downloading the entire history.
# We clone from a specific SHA (would ideally use a release tag except this upstream repository has none) in
# order to ensure that the build is reproducible and not affected by changes in the repository.

RUN apk add --no-cache git

RUN git init /tmp/mysql-proxy \
    && cd /tmp/mysql-proxy/ \
    && git fetch --depth 1 https://github.com/cppforlife/mysql-proxy 48650d9084fa404bd1d3bac2cc90f254e71ca73b \
    && git checkout FETCH_HEAD \
    && go build -v proxy.go

# Second stage of build creates a small image with just base alpine and the compiled binary.

FROM alpine:3.21

COPY --from=build /tmp/mysql-proxy/proxy /usr/local/bin/
