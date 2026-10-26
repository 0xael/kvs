.PHONY: proto build test clean

PROTO_DIR := proto
GEN_DIR := gen

proto:
	protoc --proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/ghostkv/v1/*.proto

build: proto
	go build -o bin/ghostkv-server ./cmd/ghostkv-server
	go build -o bin/ghostkv-cli ./cmd/ghostkv-cli

test:
	go test ./internal/... -v -race

run-node1:
	./bin/ghostkv-server --node-id node-1 --config config/cluster.yaml

run-node2:
	./bin/ghostkv-server --node-id node-2 --config config/cluster.yaml

run-node3:
	./bin/ghostkv-server --node-id node-3 --config config/cluster.yaml

clean:
	rm -rf bin/ /tmp/ghostkv/
