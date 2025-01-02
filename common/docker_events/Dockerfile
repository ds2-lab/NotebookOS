FROM scusemua/dist-notebook-base:latest

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable. We do not add the config file anymore, as we instead mount that as a ConfigMap.
ADD out/event_forwarder event_forwarder

USER root

ENTRYPOINT [ "./event_forwarder"]