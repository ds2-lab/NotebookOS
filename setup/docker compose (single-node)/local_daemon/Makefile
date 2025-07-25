include ../../dockfiles/env.mk

all: local-daemon
	../../dockfiles/clean.sh

all-amd64: local-daemon-amd64

build-base-image:
	cd ../../dockfiles/base-image && make 

build-image-cpu-jupyter:
ifeq ($(ARCH), x86_64)
	cd ../../dockfiles/cpu-python3-amd64 && make
else
	cd ../../dockfiles/cpu-python3-arm64 && make
endif 

build-image-gateway:
	cd ../../dockfiles/gateway && make

build-local-daemon-linux:
	GOOS=linux go build -buildvcs=false -o local_daemon ../../local_daemon

build: build-local-daemon-linux
	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
	rm local_daemon
	../../dockfiles/clean.sh

build-no-clean: build-local-daemon-linux
	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
	rm local_daemon

local-daemon-amd64: build-base-image build-image-cpu-jupyter build-image-gateway build
# use scale = num-replicas + 1 for membership tests.
	docker compose up -d --scale daemon=`./load-num-replicas.sh 1`

local-daemon: build-image-cpu-jupyter build-image-gateway build
# use scale = num-replicas + 1 for membership tests.
	docker compose up -d --scale daemon=`./load-num-replicas.sh 1`

clean:
	docker compose stop
	docker compose rm
	docker builder prune

clean-all: clean
	docker compose down -v
	docker rmi $(DOCKER_USER)daemon
	docker rmi $(DOCKER_USER)gateway
	docker rmi $(DOCKER_USER)jupyter