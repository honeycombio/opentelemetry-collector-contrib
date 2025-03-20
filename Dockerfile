FROM golang:1.23 AS otelcontribcol
WORKDIR /src
ENV CGO_ENABLED=0
COPY . .
# remove tools directory to ensure ocb is built w/ correct arch
RUN rm -Rf ./.tools
RUN make otelcontribcol

FROM ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-opampsupervisor AS opampsupervisor

FROM alpine:latest

ARG USER_UID=10001
ARG USER_GID=10001
USER ${USER_UID}:${USER_GID}

COPY --from=opampsupervisor --chmod=755 /usr/local/bin/opampsupervisor /opampsupervisor
COPY --from=otelcontribcol --chmod=755 /src/bin/otelcontribcol_linux_* /otelcol-contrib
WORKDIR /var/lib/otelcol/supervisor
ENTRYPOINT ["/opampsupervisor"]
