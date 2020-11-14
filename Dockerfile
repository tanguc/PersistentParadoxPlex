FROM rust:1.46 as build

COPY . .
# RUN rustup toolchain install nightly-2020-08-28
# RUN rustup default nightly-2020-08-28
# RUN cargo build --release
RUN mkdir -p target/release
RUN touch target/release/persistent_paradox_plex
RUN touch config.toml

FROM rust:1.46-slim
WORKDIR /
COPY --from=build target/release/persistent_paradox_plex /persistent_paradox_plex
COPY --from=build config.toml config.toml

ENTRYPOINT [ "./persistent_paradox_plex" ]

EXPOSE 7999
EXPOSE 8080