FROM quay.io/jupyter/base-notebook:latest
# FROM quay.io/jupyter/scipy-notebook:latest

# IMPORTANT: This is expected to be built using the provided Makefile from the build context ../.. 

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC \
    JUPYTER_ALLOW_INSECURE_WRITES=1 \
    GOTRACEBACK=crash \
    PYTHONFAULTHANDLER=1 
    # PYTHONASYNCIODEBUG=1

# Install amd version of pytorch and torchvision
# RUN python3 -m pip install torch torchvision
RUN python3 -m pip install protobuf==4.21.9 grpcio==1.50.0 grpcio-tools==1.50.0 debugpy

# Prepare directories to have them owned by the user jovyan
RUN mkdir -p /home/jovyan/.ipython/profile_default

# Switch to root to install the kernel
USER root:root

# Prepare directories to have them owned by the user jovyan
RUN mkdir -p /storage && chown jovyan:users /storage

# For debugging.
RUN apt-get update && apt-get install iputils-ping dnsutils unzip python3-pip vim -y
# RUN python3 -m pip install kubernetes 

# Install frontend extension.
# RUN cd /home/jovyan \
#     && git clone https://github.com/Scusemua/DistributedKernelFrontendExtension.git \
#     && cd ./DistributedKernelFrontendExtension \
#     && pip install .

# RUN mkdir /home/jovyan/data && cd /home/jovyan/data && wget http://cs231n.stanford.edu/tiny-imagenet-200.zip && unzip tiny-imagenet-200.zip
# RUN chmod -R 777 /home/jovyan/data/*

# Add files
# ADD common/gateway /distributed_notebook/common/gateway
ADD common/proto /distributed_notebook/common/proto
ADD distributed_notebook /distributed_notebook/distributed_notebook
ADD README.md /distributed_notebook/README.md
ADD MANIFEST.in /distributed_notebook/MANIFEST.in
ADD setup.py /distributed_notebook/setup.py
ADD install_kernel.sh /distributed_notebook/install_kernel.sh
ADD Makefile /distributed_notebook/Makefile
ADD dockfiles/cpu-python3-amd64/sample_notebooks/ /home/jovyan/sample_notebooks/

ADD dockfiles/cpu-python3-amd64/apport-settings /home/jovyan/.config/apport/settings

RUN chmod -R 777 /home/jovyan/sample_notebooks/*
RUN chown jovyan /home/jovyan/sample_notebooks/*

# RUN echo "kernel.core_pattern=/tmp/cores/scheduling.%e.%p.%h.%t" | sudo tee -a /etc/sysctl.conf
# RUN sudo bash -c 'echo "/tmp/cores/scheduling.%e.%p.%h.%t" > /proc/sys/kernel/core_pattern'

# Install kernel
RUN cd /distributed_notebook \
    && python3 -m grpc_tools.protoc -Icommon/proto \
    --python_out=distributed_notebook/gateway \
    --grpc_python_out=distributed_notebook/gateway \
    common/proto/gateway.proto \
    && sed -Ei 's/import gateway_pb2 as gateway__pb2/from . import gateway_pb2 as gateway__pb2/g' distributed_notebook/gateway/gateway_pb2_grpc.py
RUN cd /distributed_notebook && ./install_kernel.sh

RUN mkdir /cores
RUN chmod -R 777 /cores 
RUN chown jovyan /cores

# Copy the modified config file.
COPY dockfiles/cpu-python3-amd64/jupyter_server_config.py /home/jovyan/.jupyter/jupyter_server_config.py

EXPOSE 5678

# Switch back to jovyan
USER jovyan:users

# ENTRYPOINT ["/usr/bin/env"]
# CMD ["tini" "-g" "-vvv" "--" "start.sh"]

# ENTRYPOINT ["tini", "-g", "-vvv", "--", "start.sh"]
#CMD ["start-notebook.py"]