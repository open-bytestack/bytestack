#!/usr/bin/env bash

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ROOT_DIR="$SCRIPT_DIR"/..

DOCKER_IMAGE="repo.huoys.com/common/proto-grpc-helper-go-1-19-13:latest"

set -o errexit
set -o nounset
set -o pipefail
set -o errtrace

set -x
docker run --rm \
  -v "$PWD:/workdir" \
  -w /workdir \
  $DOCKER_IMAGE \
  protoc \
    --proto_path="/workdir" \
    --proto_path="/opt/include" \
    --go_out=paths=source_relative:. \
    --go-grpc_out=paths=source_relative:. \
    $(find proto -name '*.proto')
