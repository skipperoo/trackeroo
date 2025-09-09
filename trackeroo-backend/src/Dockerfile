# -------- Build Stage --------
FROM golang:1.24-alpine AS builder

# Set environment variables for Go
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first to leverage caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the Go binary
RUN go build -o trackeroo-backend cmd/trackeroo-backend/main.go

# -------- Runtime Stage --------
FROM alpine:latest

# Set working directory
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/trackeroo-backend .

# Expose the port your app listens on (adjust as needed)
EXPOSE 8080

# Command to run the application
ENTRYPOINT ["./trackeroo-backend"]

