# trawler/Dockerfile
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /trawler ./cmd/trawler

FROM gcr.io/distroless/static-debian12
COPY --from=build /trawler /trawler
ENTRYPOINT ["/trawler"]
