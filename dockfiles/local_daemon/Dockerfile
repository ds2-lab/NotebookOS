FROM docker:latest

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Add executable
ADD scheduler scheduler
ADD daemon.yml daemon.yml

EXPOSE 8080/tcp

ENTRYPOINT [ "./scheduler", "-yaml", "daemon.yml", "-debug"]