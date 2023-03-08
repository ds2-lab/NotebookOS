include ../env.mk

all: local-daemon

build-image-cpu-jupyter-arm64:
	cd ../cpu-python3-arm64 && make

build-image-gateway:
	cd ../gateway && make

build-scheduler-linux:
	GOOS=linux go build -o scheduler ../../scheduler

build: build-scheduler-linux
	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
	rm scheduler
	../clean.sh

local-daemon: build-image-cpu-jupyter-arm64 build-image-gateway build
	docker-compose up -d --scale daemon=`grep num-replicas ../gateway/gateway.yml | cut -d ':' -f 2 | sed 's/^[ \t]*//'`

clean:
	docker-compose stop
	docker-compose rm
	docker builder prune

clean-all: clean
	docker-compose down -v
	docker rmi $(DOCKER_USER)daemon
	docker rmi $(DOCKER_USER)gateway
	docker rmi $(DOCKER_USER)jupyter