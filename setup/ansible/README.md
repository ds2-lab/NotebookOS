# Ansible
This directory contains a number of Ansible playbooks that you can use to deploy a distributed notebook cluster on a set of virtual machines or server nodes.

## Usage

This section will describe how to use the provided ansible playbooks.

### Requirements

In order to run the ansible playbooks, you need to have ansible installed. We recommend installing it as follows:
``` shell
python3 -m pip install --user ansible ansible_runner fabric 
```

Once ansible is installed, you may run the playbooks using the following command structure:
``` shell 
ansible-playbook "<path to target playbook>" -i "<path to inventory ini file>" -e "@<path to ansible vars yml file>"
```

**Note**: make sure to prefix the path to the `ansible_vars.yml` file with an `"@"`.

### Configuration

You will need an `inventory_file.ini` and an `ansible_vars.yml` file. 
(The names can be changed as long as you update the commands used to run the playbooks accordingly).

#### The `ansible_vars.yml` File

The included `ansible_vars.template.yml` file provides a template you may use to create your `ansible_vars.yml` file. 
`ansible_vars.template.yml` contains a number of variables, many of which already have their values specified. These
values can be changed, but we recommend leaving them at their default values. The values that you must supply yourself
are "set" to `< description >` in `ansible_vars.template.yml`, where `< description >` is a description of how the
particular value should be set.

The main variables that you will need to define yourself are as follows:
- `gitbranch`: the branch of the `distributed-notebook` GitHub repository to use (most likely `main`).
- `remote_user`: the username that ansible should use when connecting to the target nodes/virtual machines (e.g., `ubuntu` or `ec2-user` for AWS Ubuntu and AWS Linux virtual machines, respectively).
- `ansible_ssh_private_key_file`: the path to the local SSH key that ansible can use when connecting to the target nodes or virtual machines.

#### The `inventory_file.ini` File 

This file is used to specify the target hosts (i.e., virtual machines, server nodes, etc.) onto which you would like to 
deploy the `distributed-notebook` cluster. The format of the file should be as follows:
``` ini
[vms]
host1
host2
...
hostN
```

### The Playbooks

The `ansible_setup.yml` playbook is the "primary" playbook. You may run the playbooks individually if desired, but the
simplest option is to simply run the `ansible_setup.yml` playbook, which will execute all the other playbooks one
after another.

``` shell 
ansible-playbook /home/ubuntu/go/pkg/distributed-notebook/setup/ansible/ansible_setup.yml \
 -i "/home/ubuntu/go/pkg/distributed-notebook/setup/ansible/inventory_file.ini" \
 -e "@/home/ubuntu/go/pkg/distributed-notebook/setup/ansible/ansible_vars.yml"
```

Prior to running any of the playbooks, however, you should first set up your configuration. Please refer to the previous
section for information regarding how to set up your configuration.

#### A Note on Compatibility

**Note**: the playbooks were tested on AWS EC2 virtual machines running Ubuntu 24.04.1 LTS. If you would like to use 
the playbooks for a different Linux distribution, then they may require some modification, such as in the parts that 
use `apt` to install dependencies. (Other distributions may use `yum`, for example.)