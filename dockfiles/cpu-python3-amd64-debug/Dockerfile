FROM quay.io/jupyter/base-notebook:latest
# FROM quay.io/jupyter/scipy-notebook:latest

# IMPORTANT: This is expected to be built using the provided Makefile from the build context ../.. 

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC \
    JUPYTER_ALLOW_INSECURE_WRITES=1 \
    GOTRACEBACK=crash \
    PYTHONFAULTHANDLER=1 
    # PYTHONASYNCIODEBUG=1

# Switch to root to build debug-enabled version of python
USER root:root

RUN sudo apt-get update 

RUN sudo apt-get --assume-yes install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev

WORKDIR /tmp 
RUN wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz 
RUN tar -xf Python-3.11.9.tgz
WORKDIR /tmp/Python-3.11.9
RUN mkdir debug 
WORKDIR /tmp/Python-3.11.9/debug
RUN ../configure --with-pydebug --enable-shared 
RUN make -j$(nproc) EXTRA_CFLAGS="-DPy_REF_DEBUG" 
RUN make -j$(nproc) altinstall

RUN sudo apt-get clean

RUN mv /tmp/Python-3.11.9/ /home/jovyan/Python-3.11.9/ 
RUN chown -R jovyan /home/jovyan/Python-3.11.9/ 
RUN chmod -R 777 /home/jovyan/Python-3.11.9/  

ENV LD_LIBRARY_PATH=/home/jovyan/Python-3.11.9/debug

# Install amd version of pytorch and torchvision
# RUN python3 -m pip install torch torchvision
RUN /home/jovyan/Python-3.11.9/debug/python -m pip install protobuf==4.21.9 grpcio==1.50.0 grpcio-tools==1.50.0 debugpy

# Switch back to jovyan
USER jovyan:users

# Prepare directories to have them owned by the user jovyan
RUN mkdir -p /home/jovyan/.ipython/profile_default

# Switch to root to install the kernel
USER root:root

# Prepare directories to have them owned by the user jovyan
RUN mkdir -p /storage && chown jovyan:users /storage

# For debugging.
RUN apt-get update && apt-get install iputils-ping dnsutils unzip python3-pip vim gdb -y
# RUN python3 -m pip install kubernetes 

# Install frontend extension.
# RUN cd /home/jovyan \
#     && git clone https://github.com/Scusemua/DistributedKernelFrontendExtension.git \
#     && cd ./DistributedKernelFrontendExtension \
#     && pip install .

# RUN mkdir /home/jovyan/datasets && cd /home/jovyan/datasets && wget http://cs231n.stanford.edu/tiny-imagenet-200.zip && unzip tiny-imagenet-200.zip
# RUN chmod -R 777 /home/jovyan/datasets/*

RUN /home/jovyan/Python-3.11.9/debug/python -m pip install jupyter jupyterlab jupyter_server jsonpatch

# Add files
ADD common/gateway /distributed_notebook/common/gateway
ADD distributed_notebook /distributed_notebook/distributed_notebook
ADD README.md /distributed_notebook/README.md
ADD MANIFEST.in /distributed_notebook/MANIFEST.in
ADD setup.py /distributed_notebook/setup.py
ADD install_kernel_debug.sh /distributed_notebook/install_kernel_debug.sh
ADD Makefile /distributed_notebook/Makefile
ADD dockfiles/cpu-python3-amd64-debug/sample_notebooks/ /home/jovyan/sample_notebooks/

ADD dockfiles/cpu-python3-amd64-debug/apport-settings /home/jovyan/.config/apport/settings

RUN mkdir /cores
RUN chmod -R 777 /cores 
RUN chown jovyan /cores

RUN chmod -R 777 /home/jovyan/sample_notebooks/*
RUN chown jovyan /home/jovyan/sample_notebooks/*

# RUN echo "kernel.core_pattern=/tmp/cores/core.%e.%p.%h.%t" | sudo tee -a /etc/sysctl.conf
# RUN sudo bash -c 'echo "/tmp/cores/core.%e.%p.%h.%t" > /proc/sys/kernel/core_pattern'

# Install kernel
RUN cd /distributed_notebook \
    && /home/jovyan/Python-3.11.9/debug/python -m grpc_tools.protoc -Icommon/gateway \
    --python_out=distributed_notebook/gateway \
    --grpc_python_out=distributed_notebook/gateway \
    common/gateway/gateway.proto \
    && sed -Ei 's/import gateway_pb2 as gateway__pb2/from . import gateway_pb2 as gateway__pb2/g' distributed_notebook/gateway/gateway_pb2_grpc.py
RUN cd /distributed_notebook && ./install_kernel_debug.sh

ADD dockfiles/cpu-python3-amd64-debug/start-notebook.sh /usr/local/bin/start-notebook.sh 
RUN chmod 777 /usr/local/bin/start-notebook.sh 
RUN chown jovyan /usr/local/bin/start-notebook.sh 

ADD dockfiles/cpu-python3-amd64-debug/start-notebook.py /usr/local/bin/start-notebook.py 
RUN chmod 777 /usr/local/bin/start-notebook.py 
RUN chown jovyan /usr/local/bin/start-notebook.py 

# Copy the modified config file.
COPY dockfiles/cpu-python3-amd64-debug/jupyter_server_config.py /home/jovyan/.jupyter/jupyter_server_config.py

ENV PATH=/usr/local/bin:/opt/conda/condabin:/opt/conda/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin

EXPOSE 5678

# Switch back to jovyan
USER jovyan:users

WORKDIR /home/jovyan/

# ENTRYPOINT ["/usr/bin/env"]
# CMD ["tini" "-g" "-vvv" "--" "start.sh"]

# ENTRYPOINT ["tini", "-g", "-vvv", "--", "start.sh"]
#CMD ["start-notebook.py"]

# ENTRYPOINT ["/bin/bash", "/usr/local/bin/start-notebook.sh"]