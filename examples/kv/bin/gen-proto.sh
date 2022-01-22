#!/usr/bin/env bash
set -euxo pipefail
cd "$(dirname "$0")"

pushd ../proto
protoc --go_out=gen --go_opt=paths=source_relative --go-grpc_out=gen --go-grpc_opt=paths=source_relative ./*.proto
popd
