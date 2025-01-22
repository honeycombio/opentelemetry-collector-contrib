FROM otel/opentelemetry-collector-contrib:latest AS otelcol-contrib

FROM golang:1.23 AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY . .
RUN go build -trimpath -o opampsupervisor .

FROM alpine:latest
COPY --from=build --chmod=755 /src/opampsupervisor /opampsupervisor
COPY --from=otelcol-contrib --chmod=755 /otelcol-contrib /otelcol-contrib
WORKDIR /var/lib/otelcol/supervisor
ENTRYPOINT ["/opampsupervisor"]
