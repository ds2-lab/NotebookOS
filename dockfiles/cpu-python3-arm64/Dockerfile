FROM jupyter/scipy-notebook:latest

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC

# Install arm version of pytorch and torchvision
RUN python3 -m pip install torch==1.10.0 torchvision==0.11.0 -f https://torch.kmtea.eu/whl/stable.html
RUN python3 -m pip install protobuf==4.21.9 grpcio==1.50.0 grpcio-tools==1.50.0

# Prepare directories to have them owned by the user jovyan
RUN mkdir -p /home/jovyan/.ipython/profile_default

# Switch to root to install the kernel
USER root:root

# Prepare directories to have them owned by the user jovyan
RUN mkdir -p /storage && chown jovyan:users /storage

# Add files
ADD common/gateway /distributed_notebook/common/gateway
ADD distributed_notebook /distributed_notebook/distributed_notebook
ADD README.md /distributed_notebook/README.md
ADD MANIFEST.in /distributed_notebook/MANIFEST.in
ADD setup.py /distributed_notebook/setup.py
ADD install_kernel.sh /distributed_notebook/install_kernel.sh
ADD Makefile /distributed_notebook/Makefile

# Install kernel
RUN cd /distributed_notebook \
    && python3 -m grpc_tools.protoc -Icommon/gateway \
  	--python_out=distributed_notebook/gateway \
		--grpc_python_out=distributed_notebook/gateway \
    common/gateway/gateway.proto \
	&& sed -Ei 's/import gateway_pb2 as gateway__pb2/from . import gateway_pb2 as gateway__pb2/g' distributed_notebook/gateway/gateway_pb2_grpc.py
RUN cd /distributed_notebook && ./install_kernel.sh

# Switch back to jovyan
USER jovyan:users