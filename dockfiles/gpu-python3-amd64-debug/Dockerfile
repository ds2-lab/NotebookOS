FROM scusemua/base-jupyter-image:python3.12.6-cuda

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC \
    JUPYTER_ALLOW_INSECURE_WRITES=1 \
    GOTRACEBACK=CRASH  \
    PYTHONFAULTHANDLER=1

# Add files
ADD common/proto /distributed_notebook/common/proto
ADD distributed_notebook /distributed_notebook/distributed_notebook
ADD pyproject.toml /distributed_notebook/distributed_notebook/pyproject.toml
ADD README.md /distributed_notebook/README.md
ADD MANIFEST.in /distributed_notebook/MANIFEST.in
ADD setup.py /distributed_notebook/setup.py
ADD install_kernel_debug.sh /distributed_notebook/install_kernel_debug.sh
ADD install_kernel_debug_py3-12-6.sh /distributed_notebook/install_kernel_debug_py3-12-6.sh
ADD Makefile /distributed_notebook/Makefile
ADD dockfiles/cpu-python3-amd64-debug/sample_notebooks/ /home/jovyan/sample_notebooks/

ADD dockfiles/cpu-python3-amd64-debug/apport-settings /home/jovyan/.config/apport/settings

RUN chmod -R 777 /home/jovyan/sample_notebooks/*
RUN chown jovyan /home/jovyan/sample_notebooks/*

RUN mkdir -p /home/jovyan/.cache/distributed_notebook/

RUN chmod -R 777 /home/jovyan/.cache/
RUN chown -R jovyan /home/jovyan/.cache/

# Install distributed_notebook module.
RUN /home/jovyan/Python-3.12.6/debug/python -m pip install /distributed_notebook/distributed_notebook

# Install kernel
RUN cd /distributed_notebook \
    && /home/jovyan/Python-3.12.6/debug/python -m grpc_tools.protoc -Icommon/proto \
    --python_out=distributed_notebook/gateway \
    --grpc_python_out=distributed_notebook/gateway \
    common/proto/gateway.proto \
    && sed -Ei 's/import gateway_pb2 as gateway__pb2/from . import gateway_pb2 as gateway__pb2/g' distributed_notebook/gateway/gateway_pb2_grpc.py

# RUN cd /distributed_notebook && ./install_kernel_debug.sh
RUN cd /distributed_notebook && ./install_kernel_debug_py3-12-6.sh

# Add .screenrc config file for screen.
ADD dockfiles/cpu-python3-amd64-debug/.screenrc /home/jovyan/.screenrc

# Add gdbinit configuration file.
ADD dockfiles/cpu-python3-amd64-debug/gdbinit /home/jovyan/.config/gdb/gdbinit

# Add modified start-notebook.sh script.
ADD dockfiles/cpu-python3-amd64-debug/start-notebook.sh /usr/local/bin/start-notebook.sh 
RUN chmod 777 /usr/local/bin/start-notebook.sh 
RUN chown jovyan /usr/local/bin/start-notebook.sh 

# Add modified start-notebook.py file.
ADD dockfiles/cpu-python3-amd64-debug/start-notebook.py /usr/local/bin/start-notebook.py 
RUN chmod 777 /usr/local/bin/start-notebook.py 
RUN chown jovyan /usr/local/bin/start-notebook.py 

# Add modified start.sh script.
ADD dockfiles/cpu-python3-amd64-debug/start.sh /usr/local/bin/start.sh
RUN chmod 777 /usr/local/bin/start.sh
RUN chown jovyan /usr/local/bin/start.sh

# Copy the modified config file.
COPY dockfiles/cpu-python3-amd64-debug/jupyter_server_config.py /home/jovyan/.jupyter/jupyter_server_config.py

# Make sure the jovyan user owns everything and can write to everything in their home directory.
RUN chmod -R 777 /home/jovyan
RUN chown -R jovyan /home/jovyan

# Update path.
ENV PATH=/usr/local/go/bin:/usr/local/bin:/opt/conda/condabin:/opt/conda/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin

# Set the value of the PYTHONUNBUFFERED environment variable.
ENV PYTHONUNBUFFERED=1

EXPOSE 5678

# Switch back to jovyan
USER jovyan:users

WORKDIR /home/jovyan/