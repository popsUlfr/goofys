#!/bin/bash

docker run --rm \
  -v "$PWD":/go/src/github.com/kahing/goofys \
  -w /go/src/github.com/kahing/goofys \
  golang:latest \
  bash -c "go get -d -v && make build"
