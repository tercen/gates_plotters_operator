#FROM tercen/rust-nightly:buster-slim as builder
FROM rust:1.77.2-slim-buster as builder

# need by custom build
RUN apt update && apt install -y pkg-config libssl-dev clang protobuf-compiler gcc make \
     libfontconfig1-dev fontconfig libfontconfig-dev

ARG BUILD_RUSTFLAGS

ENV RUSTFLAGS=$BUILD_RUSTFLAGS

RUN echo RUSTFLAGS=$RUSTFLAGS
WORKDIR /code
COPY . .
RUN cargo test
RUN cargo build --release

FROM debian:buster-slim
COPY --from=builder /code/target/release/gates_plotters_operator /usr/local/bin/gates_plotters_operator
CMD ["gates_plotters_operator"]

