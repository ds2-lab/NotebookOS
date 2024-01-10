FROM scusemua/dist-notebook-base:latest
#FROM debian:bullseye

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable.
ADD gateway gateway
# Add configuration file.
ADD gateway.yml gateway.yml
# Add add Deployment template for distributed kernel replicas.
ADD distributed-kernel-deployment-template.yml distributed-kernel-deployment-template.yml

EXPOSE 8080/tcp

ENTRYPOINT [ "./gateway", "-yaml", "gateway.yml", "-debug"]