APP_NAME=vfs
CMD_DIR=cmd
BIN_DIR=bin
PKG_DIR=vfs

.PHONY: all build run install clean test

all: build

build:
	@echo "🚀 Building CLI binary..."
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(APP_NAME) $(CMD_DIR)/main.go

run:
	@echo "🏃 Running $(APP_NAME)..."
	go run $(CMD_DIR)/main.go

install:
	@echo "📦 Installing globally..."
	go install ./$(CMD_DIR)

clean:
	@echo "🧹 Cleaning build artifacts..."
	rm -rf $(BIN_DIR)

test:
	@echo "🧪 Running tests..."
	go test ./$(PKG_DIR)/...

