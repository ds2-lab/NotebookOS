include ../env.mk

all: docker

# This actually builds the executable.
# Ideally, you should use the 'build-in-container' (or 'docker') recipes,
# as these build the executable within the base image, so there are no glibc errors.
build-local-daemon-linux:
	GOOS=linux go build -buildvcs=false -o local_daemon ../../local_daemon 

# This moves to the root project directory and executes `make build-gateway`
# This mounts the project within the 'base image' docker container and builds the gateway there.
# The process of building the gateway calls `make build-gateway-linux` in this directory.
build-in-container:
	cd ../../ && make build-local_daemon

docker: build-in-container
	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
	rm local_daemon
# ../clean.sh

clean:
	docker builder prune

# all: local-daemon
# 	../clean.sh

# all-amd64: local-daemon-amd64

# build-base-image:
# 	cd ../base-image && make 

# build-image-cpu-jupyter:
# ifeq ($(ARCH), x86_64)
# 	cd ../cpu-python3-amd64 && make
# else
# 	cd ../cpu-python3-arm64 && make
# endif 

# build-image-cpu-jupyter-arm64:
# 	cd ../cpu-python3-arm64 && make

# build-image-cpu-jupyter-amd64:
# 	cd ../cpu-python3-amd64 && make

# build-image-gateway:
# 	cd ../gateway && make

# build-local-daemon-linux:
# 	GOOS=linux go build -buildvcs=false -o local_daemon ../../local_daemon

# build: build-local-daemon-linux
# 	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
# 	rm local_daemon
# 	../clean.sh

# build-no-clean: build-local-daemon-linux
# 	docker build ${PARAMS} -t $(DOCKER_USER)daemon .
# 	rm local_daemon

# local-daemon-amd64: build-base-image build-image-cpu-jupyter build-image-gateway build
# use scale = num-replicas + 1 for membership tests.
	# docker compose up -d --scale daemon=`./load-num-replicas.sh 1`

# local-daemon: build-image-cpu-jupyter build-image-gateway build
# # use scale = num-replicas + 1 for membership tests.
# 	docker compose up -d --scale daemon=`./load-num-replicas.sh 1`

clean:
	docker compose stop
	docker compose rm
	docker builder prune

clean-all: clean
	docker compose down -v
	docker rmi $(DOCKER_USER)daemon
	docker rmi $(DOCKER_USER)gateway
	docker rmi $(DOCKER_USER)jupyter