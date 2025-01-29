FROM scusemua/generic-base-jupyter-image:python-3.12.6

# IMPORTANT: This is expected to be built using the provided Makefile from the build context ../../..

ENV LANG=C.UTF-8 \
    TZ=Etc/UTC \
    JUPYTER_ALLOW_INSECURE_WRITES=1 \
    GOTRACEBACK=CRASH  \
    PYTHONFAULTHANDLER=1
    # PYTHONASYNCIODEBUG=1

# Switch to root to build debug-enabled version of python
USER root:root

RUN sudo apt-get update

ENV LD_LIBRARY_PATH=/home/jovyan/Python-3.12.6/debug

ADD dockfiles/base-jupyter-image/cpu/distributed_notebooks_requirements.txt /home/jovyan/distributed_notebook_requirements.txt

RUN /home/jovyan/Python-3.12.6/debug/python -m pip install -r /home/jovyan/distributed_notebook_requirements.txt

# Update path.
ENV PATH=/usr/local/go/bin:/usr/local/bin:/opt/conda/condabin:/opt/conda/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin