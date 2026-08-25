FROM golang:1.27-alpine AS build

WORKDIR /src
COPY . .

WORKDIR /src/examples/durable-postgres-nats
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags="-s -w" -o /out/gomessenger-demo .

FROM scratch

USER 65532:65532
COPY --from=build /out/gomessenger-demo /gomessenger-demo

ENTRYPOINT ["/gomessenger-demo"]
