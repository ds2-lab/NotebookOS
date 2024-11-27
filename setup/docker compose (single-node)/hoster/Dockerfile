FROM frolvlad/alpine-python3

# SOURCE: https://github.com/dvddarias/docker-hoster

RUN pip3 install docker
RUN mkdir /hoster
WORKDIR /hoster
ADD deploy/docker-WSL2/hoster/hoster.py /hoster/

CMD ["python3", "-u", "hoster.py"]