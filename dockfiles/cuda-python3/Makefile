DOCKER_USER = zhangjyr/
CUDA_VERSION = 11.7.0-devel-ubuntu18.04

all: build

build:
	docker build --build-arg CUDA_VERSION=$(CUDA_VERSION) ${PARAMS} -t $(DOCKER_USER)cuda-python3 .

clean:
	docker builder prune