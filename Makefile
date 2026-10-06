.PHONY: test race lint fmt

test:
	go test ./...

race:
	go test -race ./...

lint:
	go vet ./...

fmt:
	gofmt -w *.go
