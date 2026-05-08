FROM debian:bookworm-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG PROTOC_VERSION=29.3
ARG GO_VERSION=1.24.13
ARG RUST_VERSION=1.81.0
ARG PROTOC_GEN_GO_VERSION=v1.34.1
ARG PROTOC_GEN_GO_GRPC_VERSION=v1.3.0
ARG PYTHON_VERSION=3.11
ARG GRPCIO_VERSION=1.74.0
ARG GRPCIO_TOOLS_VERSION=1.74.0
ARG PROTOBUF_PYTHON_VERSION=6.31.1

ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV GOPATH=/usr/local/go-workspace
ENV PATH=/usr/local/go/bin:/usr/local/go-workspace/bin:/usr/local/cargo/bin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        python${PYTHON_VERSION} \
        python3-pip \
        unzip \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /tmp/protoc.zip \
      "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" \
    && unzip /tmp/protoc.zip -d /usr/local \
    && rm /tmp/protoc.zip

RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
      | tar -C /usr/local -xz

RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION} --profile minimal

RUN python${PYTHON_VERSION} -m pip install --no-cache-dir --break-system-packages \
      grpcio==${GRPCIO_VERSION} \
      grpcio-tools==${GRPCIO_TOOLS_VERSION} \
      protobuf==${PROTOBUF_PYTHON_VERSION}

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION} \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}

RUN chmod -R a+rwx /usr/local/cargo /usr/local/rustup /usr/local/go-workspace

WORKDIR /workdir

CMD ["/bin/bash"]
