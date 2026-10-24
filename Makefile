.PHONY: all fmt vet generate test race integration chaos bench demo clean

all: fmt vet test

fmt:
	go fmt ./...

vet:
	go vet ./...

generate:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/raftkv/v1/raftkv.proto

test:
	go test -v -count=1 ./...

race:
	go test -v -race -count=1 ./...

integration:
	go test -v -count=1 ./tests/integration/...

chaos:
	go test -v -count=1 ./tests/chaos/...

bench:
	go test -v -bench=. -benchmem ./...

demo:
	bash scripts/demo.sh

clean:
	rm -rf bin/ data/ *.lock *.wal *.snap tmp/
