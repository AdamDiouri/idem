FROM golang:1.25 AS build
WORKDIR /src
ENV GOTOOLCHAIN=local CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -o /idem ./cmd/idem

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /idem /idem
ENTRYPOINT ["/idem"]
