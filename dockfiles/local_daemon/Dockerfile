FROM scusemua/dist-notebook-base:latest
#FROM debian:bullseye

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable
ADD scheduler scheduler
ADD daemon.yml daemon.yml
ADD distributed-kernel-pod.yaml distributed-kernel-pod.yaml

EXPOSE 8080/tcp

ENTRYPOINT [ "./scheduler", "-yaml", "daemon.yml", "-debug", "-v"]