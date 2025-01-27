FROM quay.io/jupyter/base-notebook:latest
# FROM quay.io/jupyter/scipy-notebook:latest

# IMPORTANT: This is expected to be built using the provided Makefile from the build context ../.. 

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC \
    JUPYTER_ALLOW_INSECURE_WRITES=1 \
    GOTRACEBACK=CRASH  \
    PYTHONFAULTHANDLER=1 
    # PYTHONASYNCIODEBUG=1

# Switch to root to build debug-enabled version of python
USER root:root

RUN sudo apt-get update

RUN sudo apt-get --assume-yes install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev lzma lzma-dev liblzma-dev

WORKDIR /tmp 
RUN wget https://www.python.org/ftp/python/3.12.6/Python-3.12.6.tgz 
RUN tar -xf Python-3.12.6.tgz
WORKDIR /tmp/Python-3.12.6
RUN mkdir debug 
WORKDIR /tmp/Python-3.12.6/debug
RUN ../configure --with-pydebug --enable-shared 
RUN make -j$(nproc) EXTRA_CFLAGS="-DPy_REF_DEBUG" 
RUN make -j$(nproc) altinstall

RUN sudo apt-get clean

RUN mv /tmp/Python-3.12.6/ /home/jovyan/Python-3.12.6/ 
RUN chown -R jovyan /home/jovyan/Python-3.12.6/ 
RUN chmod -R 777 /home/jovyan/Python-3.12.6/  

ENV LD_LIBRARY_PATH=/home/jovyan/Python-3.12.6/debug

RUN /home/jovyan/Python-3.12.6/debug/python -m pip install protobuf==5.28.1 grpcio==1.66.1 grpcio-tools==1.66.1

# Switch back to jovyan
USER jovyan:users

# Prepare directories to have them owned by the user jovyan
RUN mkdir -p /home/jovyan/.ipython/profile_default

# Switch to root to install the kernel
USER root:root

# Prepare directories to have them owned by the user jovyan
RUN mkdir -p /storage && chown jovyan:users /storage

# For debugging.
RUN apt-get update && apt-get install iputils-ping dnsutils unzip vim gdb screen sox -y

ADD dockfiles/base-jupyter-image/distributed_notebooks_requirements.txt /home/jovyan/distributed_notebook_requirements.txt

RUN /home/jovyan/Python-3.12.6/debug/python -m pip install torch torchdata torchaudio transformers datasets sox librosa

RUN /home/jovyan/Python-3.12.6/debug/python -m pip install -r /home/jovyan/distributed_notebook_requirements.txt

# Install Golang for debugging.
RUN cd /tmp && wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz && rm -rf /usr/local/go && tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz

RUN mkdir /cores
RUN chmod -R 777 /cores 
RUN chown jovyan /cores

# Update path.
ENV PATH=/usr/local/go/bin:/usr/local/bin:/opt/conda/condabin:/opt/conda/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin