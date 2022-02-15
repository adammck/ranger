#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

foreman start -m controller=1,proxy=1,node=3
