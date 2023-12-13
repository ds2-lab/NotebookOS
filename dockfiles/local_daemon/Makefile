include ../env.mk

all: local-daemon

all-amd64: local-daemon-amd

build-image-cpu-jupyter-arm64:
	cd ../cpu-python3-arm64 && make

build-image-cpu-jupyter-amd64:
	cd ../cpu-python3-amd64 && make

build-image-gateway:
	cd ../gateway && make

build-scheduler-linux:
	GOOS=linux go build -o scheduler ../../scheduler

build: build-scheduler-linux
	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
	rm scheduler
	../clean.sh

local-daemon-amd64: build-image-cpu-jupyter-amd64 build-image-gateway build
# use scale = num-replicas + 1 for membership tests.
	docker-compose up -d --scale daemon=`./load-num-replicas.sh 1`

local-daemon: build-image-cpu-jupyter-arm64 build-image-gateway build
# use scale = num-replicas + 1 for membership tests.
	docker-compose up -d --scale daemon=`./load-num-replicas.sh 1`

clean:
	docker-compose stop
	docker-compose rm
	docker builder prune

clean-all: clean
	docker-compose down -v
	docker rmi $(DOCKER_USER)daemon
	docker rmi $(DOCKER_USER)gateway
	docker rmi $(DOCKER_USER)jupyter