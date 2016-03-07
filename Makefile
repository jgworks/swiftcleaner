## simple makefile to log workflow
.PHONY: all test clean build install
GO=go
GOFLAGS ?= $(GOFLAGS:)

all: build


build: fmt vet
	$(GO) build $(GOFLAGS) ./cmd/swiftcleaner/...
	$(GO) build $(GOFLAGS) ./cmd/swiftstats/...

linux:
	GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) ./cmd/swiftcleaner/...
	GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) ./cmd/swiftstats/...

fmt:
	$(GO) fmt $(GOFLAGS) ./...

imports:
	$(GO)imports -l -w .

vet:
	$(GO) vet $(GOFLAGS) ./...

install:
	$(GO) get $(GOFLAGS) ./...

upgrade:
	$(GO) get -u $(GOFLAGS) ./...

test: install
	$(GO) test $(GOFLAGS) ./...

bench: install
	$(GO) test -run=NONE -bench=. $(GOFLAGS) ./...

clean:
	$(GO) clean $(GOFLAGS) -i ./...

## EOF
