bundle: clean build

build:
	go build ./...

test:
	mkdir -p ./coverage
	go vet ./...
	go test ./... -coverprofile ./coverage/profile

coverage: test view_coverage

view_coverage:
	go tool cover -html ./coverage/profile -o ./coverage/index.html
	open ./coverage/index.html

format:
	go fmt ./...

lint:
	go vet ./...

clean:
	rm -rf dist
