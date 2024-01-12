FROM scusemua/dist-notebook-base:latest
#FROM debian:bullseye

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable. We do not add the config file anymore, as we instead mount that as a ConfigMap.
ADD gateway gateway

# Add the StatefulSet template for distributed kernel replicas.
ADD distributed-kernel-stateful-set-template.yaml distributed-kernel-stateful-set-template.yaml
# Add the ConfigMap template. This contains the Jupyter connection file and config file information.
ADD kernel-configmap.yaml kernel-configmap.yaml

EXPOSE 8080/tcp

ENTRYPOINT [ "./gateway", "-yaml", "gateway.yml", "-debug"]