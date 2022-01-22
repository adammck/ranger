#!/usr/bin/env bash
set -euo pipefail

case $# in
  "3")
    #set -x
    echo '{"key": "'"$(echo -n "$2" | base64)"'", "value": "'"$(echo -n "$3" | base64)"'"}'\
    | grpcurl -plaintext -d @ localhost:"$1" kv.KV.Put
    ;;

  *)
    echo "Usage: $0 port key value"
    exit 1
    ;;
esac
