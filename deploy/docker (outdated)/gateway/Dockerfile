from scusemua/gateway:latest 

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable. We do not add the config file anymore, as we instead mount that as a ConfigMap.
ADD ./gateway.yml gateway.yml 

EXPOSE 8080/tcp
EXPOSE 9996/tcp 
EXPOSE 8079/tcp 

ENTRYPOINT [ "./gateway", "-yaml", "gateway.yml", "-debug"]