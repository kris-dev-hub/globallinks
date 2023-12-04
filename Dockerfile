FROM golang:1.21.4-alpine3.18

RUN apk add --no-cache bash coreutils gzip

WORKDIR /app
COPY . /app
RUN go build -o importer cmd/importer/main.go
