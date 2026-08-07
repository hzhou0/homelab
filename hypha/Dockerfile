# syntax=docker/dockerfile:1
FROM rust:1.96 AS build
WORKDIR /src

COPY . .
RUN CARGO_PROFILE_RELEASE_STRIP=symbols cargo build --release -p hypha

# cc (not static): the glibc build links libgcc_s for unwinding, which the static image lacks.
FROM gcr.io/distroless/cc:nonroot
COPY --from=build /src/target/release/hypha /hypha
USER 65532:65532
ENTRYPOINT ["/hypha"]
