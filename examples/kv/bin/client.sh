#!/usr/bin/env bash
set -euo pipefail

case $# in
  "3")
    #set -x
    echo "$3" | grpcurl -plaintext -d @ localhost:"$1" "$2"
    ;;

  *)
    echo "Usage: $0 port symbol data"
    exit 1
    ;;
esac
