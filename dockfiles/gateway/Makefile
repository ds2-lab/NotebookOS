include ../env.mk

all: build

build-gateway-linux:
	GOOS=linux go build -o gateway ../../gateway

build: build-gateway-linux
	docker build ${PARAMS} -t $(DOCKER_USER)gateway .
	rm gateway
	../clean.sh

clean:
	docker builder prune