FROM alpine:latest
#FROM otel/opentelemetry-collector-contrib:latest

COPY --chmod=755 opampsupervisor /opampsupervisor
COPY --chmod=775 otelcol-contrib /otelcol-contrib
COPY config.yaml /etc/opampsupervisor/config.yaml

ENTRYPOINT ["/opampsupervisor"]
CMD ["--config", "/etc/opampsupervisor/config.yaml"]
