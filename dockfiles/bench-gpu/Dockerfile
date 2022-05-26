ARG DOCKER_USER
FROM ${DOCKER_USER}cuda-python3

# # Install python3 packages
RUN python3 -m pip install numpy cupy

ADD test.py test.py

ENTRYPOINT [ "python3", "test.py" ]