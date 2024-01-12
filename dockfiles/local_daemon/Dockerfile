FROM scusemua/dist-notebook-base:latest
#FROM debian:bullseye

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable. We do not add the config file anymore, as we instead mount that as a ConfigMap.
ADD scheduler scheduler

EXPOSE 8080/tcp

ENTRYPOINT [ "./scheduler", "-yaml", "daemon.yml", "-debug", "-v"]