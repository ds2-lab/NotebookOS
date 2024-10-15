#!/bin/bash

ansible-playbook -i "inventory_file.ini" -e "@./ansible_vars.yml" playbook.yaml --tags "start_component" --start-at-task "Create the Hadoop HDFS NameNode service."