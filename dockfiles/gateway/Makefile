include ../env.mk

all: docker

# This actually builds the executable.
# Ideally, you should use the 'build-in-container' (or 'docker') recipes,
# as these build the executable within the base image, so there are no glibc errors.
build-gateway-linux:
	GOOS=linux go build -buildvcs=false -o gateway ../../gateway 

build-gateway-linux-new:
	GOOS=linux go build -buildvcs=false -o gateway ../../gateway/cmd/main.go

# This moves to the root project directory and executes `make build-gateway`
# This mounts the project within the 'base image' docker container and builds the gateway there.
# The process of building the gateway calls `make build-gateway-linux` in this directory.
build-in-container:
	cd ../../ && make build-gateway

docker: build-in-container
	docker build ${PARAMS} -t $(DOCKER_USER)gateway .

build-in-container-new:
	cd ../../ && make build-gateway-new

docker-new: build-in-container-new
	docker build ${PARAMS} -t $(DOCKER_USER)gateway .

build-and-push: docker
	docker push $(DOCKER_USER)gateway:latest

# Don't rebuild the Gateway executable.
# If there is no `gateway` (executable) file in this directory, then this recipe will fail.
docker-no-rebuild:
	docker build ${PARAMS} -t $(DOCKER_USER)gateway .

clean:
	docker builder prune