#!/bin/bash

ansible-playbook -i "inventory_file.ini" -e "@./ansible_vars.yml" playbook.yaml --tags "start_component" --start-at-task "Delete the Hadoop HDFS DataNode directories"