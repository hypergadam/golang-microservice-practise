APP_NAME := stocks
BIN_DIR := bin
PORT := 8081
MIGRATION_DIR := $(CURDIR)/internal/migrations

ifeq ($(POSTGRES_SETUP),)
	POSTGRES_SETUP := user=postgres password=12345 dbname=stock_service_db host=localhost port=5432 sslmode=disable
endif
LOCAL_BIN=$(CURDIR)/bin

.PHONY: build run test test-integration test-integration-down clean migration-create migration-up migration-down migration-status migration-reset generate-mock docker-build install-deps vendor-proto get-deps generate-stocks-proto

build:
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BIN_DIR)
	@go build -o $(BIN_DIR)/$(APP_NAME) ./cmd/main.go

docker-build:
	@docker compose up -d --build

run: build
	@echo "Running $(APP_NAME) on port $(PORT)..."
	@./$(BIN_DIR)/$(APP_NAME) -port $(PORT)

test:
	@echo "Testing $(APP_NAME)..."
	@go test -v ./...

test-integration:
	@echo "Running integration tests with Docker..."
	@docker compose -f docker-compose.integration.yaml up --build --abort-on-container-exit --exit-code-from stocks_integration_tests
	@docker compose -f docker-compose.integration.yaml down -v

test-integration-down:
	@echo "Cleaning up integration test containers..."
	@docker compose -f docker-compose.integration.yaml down -v

clean:
	@echo "Cleaning $(APP_NAME)..."
	@rm -rf $(BIN_DIR)
	@go clean

migration-create:
	@mkdir -p $(MIGRATION_DIR)
	@goose -dir $(MIGRATION_DIR) create $(name) sql

migration-up:
	@goose -dir $(MIGRATION_DIR) postgres "$(POSTGRES_SETUP)" up

migration-down:
	@goose -dir $(MIGRATION_DIR) postgres "$(POSTGRES_SETUP)" down
		
migration-reset:
	@goose -dir $(MIGRATION_DIR) postgres "$(POSTGRES_SETUP)" reset

generate-mock:
	@go generate ./...

install-deps:
	@mkdir -p $(LOCAL_BIN)
	@GOBIN=$(LOCAL_BIN) go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	@GOBIN=$(LOCAL_BIN) go install -mod=mod google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@GOBIN=$(LOCAL_BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

vendor-proto:
	@if [ ! -d vendor.protogen/google/api ]; then \
		echo "Cloning googleapis..."; \
		git clone --depth=1 https://github.com/googleapis/googleapis.git vendor.protogen/googleapis; \
		mkdir -p vendor.protogen/google; \
		mv vendor.protogen/googleapis/google vendor.protogen/; \
		rm -rf vendor.protogen/googleapis; \
	fi

get-deps:
	go get -u google.golang.org/protobuf/cmd/protoc-gen-go
	go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc
	go get -u github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway

generate-stocks-proto:
	@mkdir -p pkg/api/stocks
	@protoc \
		-I proto \
		-I ./stocks/vendor.protogen \
		--go_out=stocks/pkg/api/stocks --go_opt=paths=source_relative \
		--go-grpc_out=stocks/pkg/api/stocks --go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=stocks/pkg/api/stocks --grpc-gateway_opt=paths=source_relative \
		proto/stocks.proto
