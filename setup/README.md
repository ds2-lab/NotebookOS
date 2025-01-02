# Setup & Installation

The `setup/` directory contains utility scripts and Ansible playbooks provided to automate/simplify the installation 
and deployment of the `distributed-notebook` cluster.

The `setup/install.sh` script can be used to install all required dependencies on a single machine. This script has been
tested and verified to be working on Windows Subsystem for Linux 2 (WSL2) using the `Ubuntu 22.04.5 LTS` distro. The host
Windows operating system was Windows 10 Version 22H2 (OS Build `19045.5011`). Full version information of the system on 
which the `setup/install.sh` script was tested and verified to work is shown below.
```shell
WSL version: 2.3.24.0
Kernel version: 5.15.153.1-2
WSLg version: 1.0.65
MSRDC version: 1.2.5620
Direct3D version: 1.611.1-81528511
DXCore version: 10.0.26100.1-240331-1435.ge-release
Windows version: 10.0.19045.5011
```

## Python

The latest version of the `distributed-notebook` system was developed and tested using Python 3.12.6. We have also
used Python 3.11.3, though there were some bugs in that release of Python that impacted `distributed-notebook`, and thus
it is highly recommended that you use Python 3.12.6 with `distributed-notebook`. Using a version of Python > 3.12.6 may
also work fine, but this has not been tested and confirmed to work.

## Hadoop Configuration

The files in `hdfs_configuration` are provided to simplify the process of configuring Hadoop HDFS. While the provided
configuration _can_ be changed, there are no guarantees regarding whether the system will function correctly with
altered a modified HDFS configuration. Likewise, the `hadoop-env` file is to be used with a system set up and configured
using the provided `install.sh` script.

The `start-dfs.sh` and `stop-dfs.sh` scripts are simply helper scripts to start and stop HDFS, particularly when 
running HDFS on a single-node deployment of the `distributed-notebook` system. Likewise, the `format-nn.sh` script
is used to format the HDFS NameNode.

## Ansible Playbooks

The `setup/ansible` directory contains a number of Ansible playbooks provided to automate the deployment of a 
single- or multi-node deployment of the `distributed-notebook` cluster. 

For details, see the `README` file in the `setup/ansible`directory.

## Docker Compose (Single-Node)

The `setup/docker compose (single-node)` directory contains a self-contained `docker compose` application suitable for 
deploying the Distributed Jupyter Notebook Cluster on a single node.