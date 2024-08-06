FROM scusemua/dist-notebook-base:latest
#FROM debian:bullseye

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable. We do not add the config file anymore, as we instead mount that as a ConfigMap.
ADD gateway gateway

# Add the ConfigMap template. This contains the Jupyter connection file and config file information.
ADD ./kernel-configmap.yaml kernel-configmap.yaml

ADD ./gateway.yml gateway.yml

EXPOSE 8080/tcp
EXPOSE 9996/tcp 
EXPOSE 8079/tcp 

ENTRYPOINT [ "./gateway", "-yaml", "gateway.yml", "-debug"]