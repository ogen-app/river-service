# syntax=docker/dockerfile:1
# river-service: a static Go binary on distroless. No CGO, no shell. Railway
# probes the HTTP health endpoints on HEALTH_ADDR.

FROM golang:1.26-bookworm AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /river-service ./cmd/river-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /river-service /usr/local/bin/river-service
ENV GRPC_ADDR=":9092" HEALTH_ADDR=":8080"
EXPOSE 9092 8080
USER nonroot
ENTRYPOINT ["/usr/local/bin/river-service"]
