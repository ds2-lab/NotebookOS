FROM docker:latest

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable
ADD gateway gateway
ADD gateway.yml gateway.yml

EXPOSE 8080/tcp

ENTRYPOINT [ "./gateway", "-yaml", "gateway.yml", "-debug"]