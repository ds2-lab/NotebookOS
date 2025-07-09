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

## Configuration

Before running any playbooks, there are a few configuration-related steps that must be performed. First, create a file called `all.yaml` in the `setup/ansible/group_vars` directory. The `all.template.yaml` file is provided as a starting point. There are 5 configuration parameters that must be specified explicitly:
- `ansible_ssh_private_key_file`: Path to a private SSH key on the computer that will be used to run the Ansible playbook(s). Used to enable access to the other VMs in the NotebookOS cluster.
- `private_key_to_upload`: Path to a private SSH key on the computer that will be used to run the Ansible playbook(s). This SSH key will be uploaded to the VMs in the NotebookOS cluster to enable SSH connectivity between them. This is useful because you may want to run some scripts or Ansible playbooks from one of the VMs once NotebookOS has been deployed.
- `public_key_to_upload`: Path to a public SSH key on the coputer that will be used to run the Ansible playbook(s). This SSH key will be uploaded to the VMs in the NotebookOS cluster to enable SSH connectivity between them. This is useful because you may want to run some scripts or Ansible playbooks from one of the VMs once NotebookOS has been deployed.
- `gitbranch`: the branch of the NotebookOS GitHub repository to use when deploying NotebookOS. You can default to using ``main''.
- `git_personal_access_token`: [GitHub personal access token (PAT)](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens) with read access to the NotebookOS GitHub repository (or the fork of the NotebookOS source code that is being used).

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

## Custom Docker Images
There are seven configuration parameters that may need to be changed if you want to use Docker images that you built yourself:
- `distributed_notebook_kernel_docker_image_name`: Docker image used by the distributed kernel replicas, which should be of the form "`DockerUser/ImageName`".
- `gateway_docker_username`: Docker username of the account associated with the Global Scheduler Docker image.
- `local_daemon_docker_username`: Docker username of the account associated with the Local Scheduler Docker image.
- `jupyter_server_docker_username`: Docker username of the account associated with the Jupyter Server Docker image. (This Docker image is generally the same as that used by the distributed kernel replicas.)
- `dashboard_backend_docker_username`: Docker username of the account associated with the cluster/admin dashboard Docker image.
- `git_username`: The username of the GitHub account that owns the NotebookOS repository. If you would like to use a fork of the NotebookOS source code, then this parameter should be changed to be the account that owns the forked repository.
- `gopy_image_name`: The username of the Docker account associated with the `gopy` Docker image, which is used to build the distributed kernel replica Docker image.

## Running the Playbooks

Once the `all.yaml` file has been created in the correct directory (i.e., `setup/ansible/group_vars`), you can begin deploying NotebookOS. First, run the playbook to create the Docker Swarm cluster:
``` shell
$ ansible-playbook -i inventory_file.ini create_docker_swarm_cluster.yaml --tags ``swarm''
```

Next, deploy the [Traefik](https://traefik.io/traefik) Docker Stack onto the Docker Swarm cluster. Traefik is an open source reverse proxy and ingress controller that NotebookOS uses to route external web traffic to the appropriate internal component. Traefik can be deployed onto the Docker Swarm cluster by executing the following command:
``` shell
  $ ansible-playbook -i inventory_file.ini redeploy_traefik_docker_stack.yaml
```

Finally, deploy the NotebookOS Docker Stack onto the Docker Swarm cluster:
``` shell
  $ ansible-playbook -i inventory_file.ini deploy_distributed_notebook_docker_stack.yaml
```

If desired, verbose Ansible logging can be enabled by setting the `ANSIBLE_STDOUT_CALLBACK` environment variable to `debug`. For example:
``` shell
  $ ANSIBLE_STDOUT_CALLBACK=debug ansible-playbook -i inventory_file.ini deploy_distributed_notebook_docker_stack.yaml
```

### Other Useful Playbooks 

The following is a list of other Ansible playbooks that may be useful while experimenting with NotebookOS:
- `pull_docker_images.yaml`: Pull the latest Docker images for the various components on all nodes in the NotebookOS cluster.
- `remove_kernel_replica_containers.yaml`: Remove any kernel replica containers running on any of the nodes in the NotebookOS cluster.
- `delete_docker_swarm_cluster.yaml`: Delete the NotebookOS Docker Swarm cluster. 
- `create_docker_swarm_cluster.yaml`: Create (or re-create) the NotebookOS Docker Swarm cluster. 
- `redeploy_traefik_docker_stack.yaml`: Redeploy Traefik on the Docker Swarm cluster.
