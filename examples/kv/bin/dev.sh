#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

go build # kv
go build "$(dirname "$0")"/../../../cmd/rangerd
foreman start -m controller=1,proxy=1,node=3
