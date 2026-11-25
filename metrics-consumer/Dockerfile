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

RUN go build -tags musl -o metrics ./cmd/consumer/main.go

FROM alpine:3.21

WORKDIR /root

RUN apk add --no-cache 

COPY --from=builder /app/metrics .

COPY .env .

CMD [ "./metrics" ]


