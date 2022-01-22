#!/usr/bin/env bash
set -euo pipefail

case $# in
  "2")
    #set -x
    echo '{"key": "'"$(echo -n "$2" | base64)"'"}'\
    | grpcurl -plaintext -d @ localhost:"$1" kv.KV.Get
    ;;

  *)
    echo "Usage: $0 port key"
    exit 1
    ;;
esac
