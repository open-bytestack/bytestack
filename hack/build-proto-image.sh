#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)

PROTO_IMAGE_TAG="${PROTO_IMAGE_TAG:-bytestack/proto-tools:local}"
PROTO_DOCKERFILE="${PROTO_DOCKERFILE:-${ROOT_DIR}/hack/proto-tools.Dockerfile}"

docker build \
  -f "${PROTO_DOCKERFILE}" \
  -t "${PROTO_IMAGE_TAG}" \
  "${ROOT_DIR}"
