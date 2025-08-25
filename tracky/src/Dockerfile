# -------- Build Stage --------
FROM golang:1.24-alpine AS builder

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o tracky tracky.go firmware.go

FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/tracky .

ENTRYPOINT ["./tracky"]
