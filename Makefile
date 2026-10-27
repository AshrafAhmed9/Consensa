.PHONY: test race vet lint torture verify

test:
	go test -p 1 ./...

race:
	go test -race -p 1 ./...

vet:
	go vet ./...

lint:
	golangci-lint run

torture:
	pytest harness/torture

verify: vet test race
	python3 -m pytest harness
