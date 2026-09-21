# syntax=docker/dockerfile:1
FROM golang:1.26.4-bookworm@sha256:b305420a68d0f229d91eb3b3ed9e519fcf2cf5461da4bef997bf927e8c0bfd2b AS dev
WORKDIR /app
ENV GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /app/bin/api ./cmd/api && CGO_ENABLED=0 go build -trimpath -o /app/bin/migrate ./cmd/migrate && ln -s /app/bin/migrate /migrate && ln -s /app/migrations /migrations
EXPOSE 8080
ENTRYPOINT ["/app/bin/api"]

FROM scratch AS release
COPY --from=dev /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=dev /app/bin/api /api
COPY --from=dev /app/bin/migrate /migrate
COPY migrations /migrations
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/api"]
