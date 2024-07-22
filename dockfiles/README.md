# Docker Files

This directory contains (a bunch of sub-directories, each of which contains) docker image definitions (i.e., Dockerfiles) and docker-compose files for deploying the Distributed Notebook Cluster on Docker.

There are two provided scripts for starting and stopping all components: `start-components.sh` and `stop-components.sh`, respectively. These scripts should be executed from this directory.

Note that the `dockfiles/local_daemon/docker-compose.yml` file contains definitions for several components, namely the local daemon, the cluster gateway, and the jupyter frontend. As such, the `dockfiles/gateway` directory does not contain its own `docker-compose.yml` file.

**Important**: You need a file in your home directory called `kernel_base` in order for the deployment to succeed/operate correctly.