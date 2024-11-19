#FROM tercen/rust-nightly:buster-slim as builder
FROM rust:1.77.2-slim-buster AS builder

# need by custom build
RUN apt update && apt install -y pkg-config libssl-dev clang protobuf-compiler gcc make \
     libfontconfig1-dev fontconfig libfontconfig-dev

ARG BUILD_RUSTFLAGS

ENV RUSTFLAGS=$BUILD_RUSTFLAGS

RUN echo RUSTFLAGS=$RUSTFLAGS
WORKDIR /code
COPY . .
#RUN cargo test
RUN cargo build --release

FROM debian:buster-slim
RUN apt update && apt install -y pkg-config \
     libfontconfig1-dev fontconfig libfontconfig-dev
COPY --from=builder /code/target/release/gates_plotters_operator /usr/local/bin/gates_plotters_operator
ENTRYPOINT ["/usr/local/bin/gates_plotters_operator"]

