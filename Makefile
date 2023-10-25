bundle: clean build

build:
	go build ./...

test:
	mkdir -p ./coverage
	go vet ./...
	go test -v ./... -coverprofile ./coverage/profile
	go tool cover -html ./coverage/profile -o ./coverage/index.html
	cd tests && go test -v ./...

coverage: test
	open ./coverage/index.html

format:
	go fmt ./...

lint:
	go vet ./...

clean:
	rm -rf dist
