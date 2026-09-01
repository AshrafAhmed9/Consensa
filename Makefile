.PHONY: test race vet lint bench torture verify

test:
	go test -p 1 ./...

race:
	go test -race -p 1 ./...

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
