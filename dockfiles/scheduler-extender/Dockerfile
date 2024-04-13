FROM scusemua/dist-notebook-base:latest
ARG VERSION=0.0.1

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

ADD scheduler_extender scheduler_extender

ENTRYPOINT ["./scheduler_extender"]