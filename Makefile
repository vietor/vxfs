MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_PATH := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))
export GOPATH = $(PROJECT_PATH)

TIMESTAMP := $(shell /bin/date "+%s")

build:
	echo $(PROJECT_PATH)
	go build -ldflags="-s -w" -o bin/vxfs-named src/cmd/vxfs-stored.go
	go build -ldflags="-s -w" -o bin/vxfs-stored src/cmd/vxfs-named.go
	go build -ldflags="-s -w" -o bin/vxfs-proxyd src/cmd/vxfs-proxyd.go

clean:
	rm -f bin/*

.PHONY: build clean
