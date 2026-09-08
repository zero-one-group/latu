#!/usr/bin/env bash
# Fails when priv/proto/ drifts from the Spark tag in priv/proto/VERSION. See dev/README.md.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
tag=$(tr -d '[:space:]' < "$root/priv/proto/VERSION")
base="https://raw.githubusercontent.com/apache/spark/$tag/sql/connect/common/src/main/protobuf/spark/connect"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

status=0
for path in "$root"/priv/proto/spark/connect/*.proto; do
  name=$(basename "$path")

  if ! curl -fsSL "$base/$name" -o "$tmp/$name"; then
    echo "GONE     $name is not in apache/spark at $tag"
    status=1
    continue
  fi

  if cmp -s "$path" "$tmp/$name"; then
    echo "ok       $name"
  else
    echo "DRIFTED  $name"
    diff -u "$path" "$tmp/$name" || true
    status=1
  fi
done

if [ "$status" -ne 0 ]; then
  cat <<'MSG'

The vendored protos are not what that Spark tag ships. Either re-vendor them and run
`mix proto.generate`, or move priv/proto/VERSION to the tag you actually mean.
MSG
fi

exit "$status"
