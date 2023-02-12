DOCKER_USER = zhangjyr/

all: local-daemon

build-scheduler-linux:
	GOOS=linux go build -o scheduler ../../scheduler

build-image-cpu-jupyter-arm64:
	docker build ${PARAMS} -t $(DOCKER_USER)jupyter -f ../cpu-python3-arm64/Dockerfile ../..

build: build-scheduler-linux
	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
	rm scheduler

local-daemon: build-image-cpu-jupyter-arm64 build
	docker-compose up -d

clean:
	docker-compose stop
	docker-compose rm
	./clean.sh
	docker builder prune