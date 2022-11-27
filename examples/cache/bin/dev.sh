#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if ! consul info 1>/dev/null 2>/dev/null; then
    echo "Error: Consul is not running"
    exit 1
fi

go build # cache
go build "$(dirname "$0")"/../../../cmd/rangerctl
go build "$(dirname "$0")"/../../../cmd/rangerd
foreman start -m controller=1,node=3
