#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)

PROTO_IMAGE_TAG="${PROTO_IMAGE_TAG:-bytestack/proto-tools:local}"
PROTO_DOCKERFILE="${PROTO_DOCKERFILE:-${ROOT_DIR}/hack/proto-tools.Dockerfile}"
DOCKER_BUILD_CMD="${DOCKER_BUILD_CMD:-docker build}"
DOCKER_RUN_CMD="${DOCKER_RUN_CMD:-docker run}"

build_image() {
  ${DOCKER_BUILD_CMD} \
    -f "${PROTO_DOCKERFILE}" \
    -t "${PROTO_IMAGE_TAG}" \
    "${ROOT_DIR}"
}

container_script() {
  cat <<'EOF'
set -euo pipefail

cd /workdir

export PATH="/usr/local/go/bin:/usr/local/go-workspace/bin:/usr/local/cargo/bin:${PATH}"

PY_PROTO_INCLUDE="$(python3 - <<'PY'
import os
import grpc_tools
print(os.path.join(os.path.dirname(grpc_tools.__file__), "_proto"))
PY
)"

generate_go() {
  local proto_file="$1"
  protoc \
    --proto_path=/workdir \
    --proto_path=/usr/local/include \
    --go_out=paths=source_relative:/workdir \
    --go-grpc_out=paths=source_relative:/workdir \
    "${proto_file}"
}

generate_python() {
  local proto_dir="$1"
  local proto_file="$2"
  python3 -m grpc_tools.protoc \
    -I"${proto_dir}" \
    -I"${PY_PROTO_INCLUDE}" \
    --python_out="${proto_dir}" \
    --grpc_python_out="${proto_dir}" \
    "${proto_file}"
}

generate_rust() {
  local out_dir="$1"
  local bin_name="$2"
  OUT_DIR="${out_dir}" cargo run --quiet --bin "${bin_name}" --manifest-path /workdir/proto/Cargo.toml
}

generate_go proto/src/controller/controller.proto
generate_go proto/src/turbo/turbo.proto

generate_python /workdir/proto/src/controller controller.proto
generate_python /workdir/proto/src/turbo turbo.proto

generate_rust /workdir/proto/src/controller gen-controller-proto
generate_rust /workdir/proto/src/turbo gen-turbo-proto
EOF
}

run_generation() {
  local docker_user_args=()
  if command -v id >/dev/null 2>&1; then
    docker_user_args+=(--user "$(id -u):$(id -g)")
  fi

  ${DOCKER_RUN_CMD} --rm \
    "${docker_user_args[@]}" \
    -v "${ROOT_DIR}:/workdir" \
    -w /workdir \
    -e HOME=/tmp/bytestack-proto-home \
    -e CARGO_TARGET_DIR=/tmp/bytestack-proto-target \
    "${PROTO_IMAGE_TAG}" \
    bash -c "$(container_script)"
}

main() {
  build_image
  run_generation
}

main "$@"
