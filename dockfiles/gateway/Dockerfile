FROM scusemua/dist-notebook-base:latest
#FROM debian:bullseye

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable.
ADD gateway gateway
# Add configuration file.
ADD gateway.yml gateway.yml
# Add the StatefulSet template for distributed kernel replicas.
ADD distributed-kernel-stateful-set-template.yaml distributed-kernel-stateful-set-template.yaml
# Add the ConfigMap template. This contains the Jupyter connection file and config file information.
ADD kernel-configmap.yaml kernel-configmap.yaml

EXPOSE 8080/tcp

ENTRYPOINT [ "./gateway", "-yaml", "gateway.yml", "-debug"]