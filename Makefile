bundle: clean build

build:
	go build ./...

test:
	mkdir -p ./coverage
	go vet ./...
	go test ./... -coverprofile ./coverage/profile
	go tool cover -html ./coverage/profile -o ./coverage/index.html

coverage: test
	open ./coverage/index.html

format:
	go fmt ./...

lint:
	go vet ./...

clean:
	rm -rf dist
