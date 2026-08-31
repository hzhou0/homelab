# syntax=docker/dockerfile:1
FROM golang:1.22 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/neon-ctl ./cmd/neon-ctl

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/neon-ctl /neon-ctl
USER 65532:65532
ENTRYPOINT ["/neon-ctl"]
