DOCKER_USER = zhangjyr/

all: build

build:
	docker build ${PARAMS} -t $(DOCKER_USER)jupyter -f Dockerfile ../..

clean:
	docker builder prune