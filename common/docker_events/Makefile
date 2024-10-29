DOCKER_USER="scusemua"

build:
	go build -buildvcs=false -o ./out/event_forwarder .
	chmod +x ./out/event_forwarder

build-linux:
	GOOS=linux go build -buildvcs=false -o ./out/event_forwarder .
	chmod +x ./out/event_forwarder

docker:
	docker build -t $(DOCKER_USER)/docker_event_forwarder:latest .

docker-push:
	docker push $(DOCKER_USER)/docker_event_forwarder:latest

clean:
	rm out/event_forwarder