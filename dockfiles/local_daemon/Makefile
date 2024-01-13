include ../env.mk

all: local-daemon
	../clean.sh

all-amd64: local-daemon-amd64

build-base-image:
	cd ../base-image && make 

build-image-cpu-jupyter:
ifeq ($(ARCH), x86_64)
	cd ../cpu-python3-amd64 && make
else
	cd ../cpu-python3-arm64 && make
endif 

# build-image-cpu-jupyter-arm64:
# 	cd ../cpu-python3-arm64 && make

# build-image-cpu-jupyter-amd64:
# 	cd ../cpu-python3-amd64 && make

build-image-gateway:
	cd ../gateway && make

build-scheduler-linux:
	GOOS=linux go build -buildvcs=false -o scheduler ../../scheduler

build: build-scheduler-linux
	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
	rm scheduler
	../clean.sh

build-no-clean: build-scheduler-linux
	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
	rm scheduler

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