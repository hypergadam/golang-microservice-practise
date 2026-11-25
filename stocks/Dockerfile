FROM golang:1.27-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add alpine-sdk
RUN apk update && apk add --no-progress --no-cache \
    git \
    gcc \
    musl-dev 

COPY go.mod go.sum ./
RUN go mod download

# Copy Source code
COPY .  .

# Build Cart service app
RUN go build -tags musl -o bin/stocks ./cmd/main.go

# Final stage
FROM alpine:3.21

WORKDIR /root

RUN apk add --no-cache

# Copy binary from builder stage
COPY --from=builder /app/bin/stocks .

COPY .env .

EXPOSE 8081

CMD [ "./stocks" ]

