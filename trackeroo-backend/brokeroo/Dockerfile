# Build stage
FROM golang:1.24-alpine AS builder

# Set working directory
WORKDIR /app

# Install git (required for go mod download)
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Build the application
RUN go build -o brokeroo cmd/brokeroo/main.go

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/brokeroo .

# Run the binary
CMD ["./brokeroo"]
