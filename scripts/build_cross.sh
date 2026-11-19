#!/usr/bin/env bash
# 交叉编译五平台 sidecar（Go portable toolchain）
set -e
export PATH="/c/asdm-tools/_tools/go/bin:$PATH"
export GOROOT="C:\asdm-tools\_tools\go"
cd "$(dirname "$0")/../backend"
mkdir -p ../bin/cross
for spec in "linux amd64 linux-x64" "linux arm64 linux-arm64" "darwin amd64 darwin-x64" "darwin arm64 darwin-arm64" "windows amd64 windows-x64"; do
  set -- $spec
  echo "building $3 ..."
  GOOS=$1 GOARCH=$2 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
    -o "../bin/cross/dbx-plugin-flink-studio-$3" .
  echo "  ok $3"
done
ls -lh ../bin/cross/
