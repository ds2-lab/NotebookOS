DOCKER_USER = zhangjyr/

all: build

build:
	docker build --build-arg DOCKER_USER=$(DOCKER_USER) ${PARAMS} -t $(DOCKER_USER)bench-gpu .

clean:
	docker builder prune