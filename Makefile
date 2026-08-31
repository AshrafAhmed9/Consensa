.PHONY: test race vet lint bench torture verify

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run

bench:
	go test -bench=. -benchmem ./internal/storage

torture:
	pytest harness/torture

verify: vet test race
	python3 -m pytest harness
