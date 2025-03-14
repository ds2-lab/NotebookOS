FROM scusemua/jupyter-cpu:latest

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC \
    JUPYTER_ALLOW_INSECURE_WRITES=1 \
    GOTRACEBACK=CRASH  \
    PYTHONFAULTHANDLER=1

USER root:root

# Make sure the jovyan user owns everything and can write to everything in their home directory.
RUN chmod -R 777 /home/jovyan
RUN chown -R jovyan /home/jovyan
RUN chmod -R 777 /distributed_notebook
RUN chown -R jovyan /distributed_notebook

# Switch back to jovyan
USER jovyan:users

# Add files
COPY --chown=jovyan:users common/proto /distributed_notebook/common/proto
COPY --chown=jovyan:users distributed_notebook /distributed_notebook/distributed_notebook
COPY --chown=jovyan:users pyproject.toml /distributed_notebook/distributed_notebook/pyproject.toml
COPY --chown=jovyan:users README.md /distributed_notebook/README.md
COPY --chown=jovyan:users MANIFEST.in /distributed_notebook/MANIFEST.in
COPY --chown=jovyan:users setup.py /distributed_notebook/setup.py
COPY --chown=jovyan:users install_kernel_debug.sh /distributed_notebook/install_kernel_debug.sh
COPY --chown=jovyan:users install_kernel_debug_py3-12-6.sh /distributed_notebook/install_kernel_debug_py3-12-6.sh
COPY --chown=jovyan:users Makefile /distributed_notebook/Makefile
COPY --chown=jovyan:users dockfiles/cpu-python3-amd64-development/sample_notebooks/ /home/jovyan/sample_notebooks/

COPY --chown=jovyan:users dockfiles/cpu-python3-amd64-development/apport-settings /home/jovyan/.config/apport/settings

# RUN mkdir -p /home/jovyan/.cache/distributed_notebook/

# Install distributed_notebook module.
# RUN /home/jovyan/Python-3.12.6/debug/python -m pip install -e /distributed_notebook/distributed_notebook

# Install kernel
RUN cd /distributed_notebook \
    && /home/jovyan/Python-3.12.6/debug/python -m grpc_tools.protoc -Icommon/proto \
    --python_out=distributed_notebook/gateway \
    --grpc_python_out=distributed_notebook/gateway \
    common/proto/gateway.proto

RUN cd /distributed_notebook \
    && sed -Ei 's/import gateway_pb2 as gateway__pb2/from . import gateway_pb2 as gateway__pb2/g' distributed_notebook/gateway/gateway_pb2_grpc.py

# RUN cd /distributed_notebook && ./install_kernel_debug.sh
# RUN cd /distributed_notebook && ./install_kernel_debug_py3-12-6.sh

# COPY --chown=jovyan:users .screenrc config file for screen.
COPY --chown=jovyan:users dockfiles/cpu-python3-amd64-development/.screenrc /home/jovyan/.screenrc

# COPY --chown=jovyan:users gdbinit configuration file.
COPY --chown=jovyan:users dockfiles/cpu-python3-amd64-development/gdbinit /home/jovyan/.config/gdb/gdbinit

# COPY --chown=jovyan:users modified start-notebook.sh script.
COPY --chown=jovyan:users dockfiles/cpu-python3-amd64-development/start-notebook.sh /usr/local/bin/start-notebook.sh

# COPY --chown=jovyan:users modified start-notebook.py file.
COPY --chown=jovyan:users dockfiles/cpu-python3-amd64-development/start-notebook.py /usr/local/bin/start-notebook.py

# COPY --chown=jovyan:users modified start.sh script.
COPY --chown=jovyan:users dockfiles/cpu-python3-amd64-development/start.sh /usr/local/bin/start.sh

# Copy the modified config file.
COPY dockfiles/cpu-python3-amd64-development/jupyter_server_config.py /home/jovyan/.jupyter/jupyter_server_config.py

# Update path.
ENV PATH=/usr/local/go/bin:/usr/local/bin:/opt/conda/condabin:/opt/conda/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin

# Set the value of the PYTHONUNBUFFERED environment variable.
ENV PYTHONUNBUFFERED=1

EXPOSE 5678

WORKDIR /home/jovyan/