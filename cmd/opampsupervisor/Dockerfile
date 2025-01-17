FROM otel/opentelemetry-collector-contrib:latest

ARG USER_UID=10001
USER ${USER_UID}

COPY --chmod=755 opampsupervisor /opampsupervisor
COPY config.yaml /etc/opampsupervisor/config.yaml
ENTRYPOINT ["/opampsupervisor"]
CMD ["--config", "/etc/opampsupervisor/config.yaml"]
