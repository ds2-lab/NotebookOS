FROM scusemua/daemon:latest

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# ADD ./daemon.yml daemon.yml 

EXPOSE 8080/tcp

ENTRYPOINT [ "./local_daemon", "-yaml", "daemon.yml", "-debug", "-v"]