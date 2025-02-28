FROM golang:1.23 AS otelcontribcol
WORKDIR /src
ENV CGO_ENABLED=0
COPY . .
# remove tools directory to ensure ocb is built w/ correct arch
RUN rm -Rf ./.tools
RUN make otelcontribcol

FROM golang:1.23 AS opampsupervisor
WORKDIR /src
ENV CGO_ENABLED=0
COPY cmd/opampsupervisor .
RUN go build -trimpath -o opampsupervisor .

FROM alpine:latest
COPY --from=opampsupervisor --chmod=755 /src/opampsupervisor /opampsupervisor
COPY --from=otelcontribcol --chmod=755 /src/bin/otelcontribcol_linux_* /otelcol-contrib
WORKDIR /var/lib/otelcol/supervisor
ENTRYPOINT ["/opampsupervisor"]
