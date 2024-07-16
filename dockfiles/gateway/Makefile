include ../env.mk

all: build

build-gateway-linux:
	GOOS=linux go build -buildvcs=false -o gateway ../../gateway 

build-in-container:
	docker run -it --rm -v ../..:/go/src/in -v `pwd`:/out scusemua/dist-notebook-base /bin/bash -c "cd /go/src/in/dockfiles/gateway && make build-smr-linux-amd64"

build: build-gateway-linux
	docker build ${PARAMS} -t $(DOCKER_USER)gateway .
	rm gateway
# ../clean.sh

clean:
	docker builder prune