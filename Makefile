# MashDB Makefile

BINARY_NAME=mashdb
BUILD_DIR=bin
CMD_DIR=cmd/mashdb

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt
GOVET=$(GOCMD) vet

# Build flags
LDFLAGS=-ldflags "-s -w"

.PHONY: all build clean test coverage fmt vet run help tidy

all: clean build test

## build: Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./$(CMD_DIR)
	@echo "Binary built at $(BUILD_DIR)/$(BINARY_NAME)"

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f *.db
	$(GOCLEAN)

## test: Run all tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

## test-short: Run tests without verbose output
test-short:
	$(GOTEST) ./...

## coverage: Run tests with coverage report
coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated at coverage.html"

## fmt: Format all Go files
fmt:
	@echo "Formatting..."
	$(GOFMT) ./...

## vet: Run go vet
vet:
	@echo "Running go vet..."
	$(GOVET) ./...

## tidy: Tidy go.mod
tidy:
	@echo "Tidying modules..."
	$(GOMOD) tidy

## run: Build and run with a test database
run: build
	./$(BUILD_DIR)/$(BINARY_NAME) test.db

## bench: Run benchmarks
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem ./...

## help: Show this help message
help:
	@echo "MashDB - A simple SQLite-like database in Go"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'
