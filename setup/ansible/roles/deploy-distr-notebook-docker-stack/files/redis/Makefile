DOCKER_USER = scusemua/

build-image:
	docker build ${PARAMS} -t $(DOCKER_USER)redis .

push-image:
	docker push $(DOCKER_USER)redis:latest

run-image:
	docker run -it $(DOCKER_USER)redis:latest