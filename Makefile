# BBgrid Makefile

# 变量
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Go 参数
GO := go
LDFLAGS := -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)"

# 目录
BIN_DIR := bin

# 平台列表
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64

.PHONY: all build build-all clean run-server run-client help runtime test vet fmt check $(PLATFORMS)

# 默认目标
all: build-all

# ==================== 当前平台编译 ====================

build: server client cli runtime

server:
	@echo "编译 BBgrid Server..."
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/bbgrid-server ./BBgrid_Server/
	@echo "编译完成: $(BIN_DIR)/bbgrid-server"

client:
	@echo "编译 BBgrid Client..."
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/bbgrid-client ./BBgrid_Client/
	@echo "编译完成: $(BIN_DIR)/bbgrid-client"

cli:
	@echo "编译 BBgrid CLI..."
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/bbgrid-cli ./BBgrid_Cmd/bbgrid-cli/
	@echo "编译完成: $(BIN_DIR)/bbgrid-cli"

runtime:
	@echo "编译 BBgrid Runtime..."
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/bbgrid-runtime ./BBgrid_Runtime/
	@echo "编译完成: $(BIN_DIR)/bbgrid-runtime"

# ==================== 全平台编译 ====================

build-all: $(PLATFORMS)

$(PLATFORMS):
	$(eval GOOS := $(word 1,$(subst /, ,$@)))
	$(eval GOARCH := $(word 2,$(subst /, ,$@)))
	@echo "编译 $(GOOS)/$(GOARCH)..."
	@mkdir -p $(BIN_DIR)/$(GOOS)-$(GOARCH)
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(GOOS)-$(GOARCH)/bbgrid-server ./BBgrid_Server/
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(GOOS)-$(GOARCH)/bbgrid-client ./BBgrid_Client/
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(GOOS)-$(GOARCH)/bbgrid-cli ./BBgrid_Cmd/bbgrid-cli/
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(GOOS)-$(GOARCH)/bbgrid-runtime ./BBgrid_Runtime/
	@echo "编译完成: $(BIN_DIR)/$(GOOS)-$(GOARCH)/"

# ==================== 代码质量 ====================

test:
	@echo "运行测试..."
	$(GO) test ./...

vet:
	@echo "运行 go vet..."
	$(GO) vet ./...

fmt:
	@echo "格式化代码..."
	$(GO) fmt ./...

check: fmt vet test
	@echo "代码检查完成"

# ==================== 其他 ====================

clean:
	@echo "清理编译产物..."
	rm -rf $(BIN_DIR)
	@echo "清理完成"

run-server: server
	@echo "启动 BBgrid Server..."
	./$(BIN_DIR)/bbgrid-server -config data/config.json

run-client: client
	@echo "启动 BBgrid Client..."
	./$(BIN_DIR)/bbgrid-client -config data/client.json

help:
	@echo "BBgrid Makefile"
	@echo ""
	@echo "用法:"
	@echo "  make              编译所有平台"
	@echo "  make build        编译当前平台 (server + client + cli + runtime)"
	@echo "  make build-all    编译所有平台"
	@echo "  make server       只编译 server"
	@echo "  make client       只编译 client"
	@echo "  make cli          只编译 cli"
	@echo "  make runtime      只编译 runtime"
	@echo "  make test         运行所有测试"
	@echo "  make vet          运行 go vet 静态检查"
	@echo "  make fmt          格式化代码"
	@echo "  make check        运行 fmt + vet + test"
	@echo "  make clean        清理编译产物"
	@echo "  make run-server   编译并运行 server"
	@echo "  make run-client   编译并运行 client"
	@echo "  make help         显示帮助"
	@echo ""
	@echo "支持的平台:"
	@echo "  linux/amd64"
	@echo "  linux/arm64"
	@echo "  darwin/amd64"
	@echo "  darwin/arm64"
	@echo "  windows/amd64"
