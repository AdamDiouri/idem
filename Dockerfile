FROM golang:1.27.4 AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Failpoints stay compiled in (inactive unless IDEM_FAILPOINTS is set) so chaos
# runs can use them; `make build-release` builds with -tags nofailpoint.
RUN go build -trimpath -o /idem ./cmd/idem && \
    go build -trimpath -o /fakeprovider ./cmd/fakeprovider

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /idem /idem
COPY --from=build /fakeprovider /fakeprovider
ENTRYPOINT ["/idem"]
