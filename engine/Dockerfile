FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /flowforged ./cmd/flowforged

FROM gcr.io/distroless/static-debian12
COPY --from=build /flowforged /flowforged
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/flowforged"]
