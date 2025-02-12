#!/bin/bash

########################################
# Update Cluster Gateway configuration #
########################################

# File to modify
target_file="./gateway/gateway.yml"

# Ensure the file exists
if [[ ! -f "$target_file" ]]; then
  echo "Error: File '$target_file' does not exist."
  exit 1
fi

# Ensure the argument is provided and valid
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <default|static|dynamic-v3|dynamic-v4|fcfs-batch|auto-scaling-fcfs-batch|reservation|gandiva|middle-ground>"
  exit 1
fi

# Get the first argument and validate
new_value=$1
if [[ "$new_value" != "default" && "$new_value" != "static" && "$new_value" != "dynamic-v3" && "$new_value" != "dynamic-v4" && "$new_value" != "fcfs-batch" && "$new_value" != "auto-scaling-fcfs-batch" && "$new_value" != "reservation" && "$new_value" != "gandiva" && "$new_value" != "middle-ground" ]]; then
  echo "Usage: $0 <default|static|dynamic-v3|dynamic-v4|fcfs-batch|auto-scaling-fcfs-batch|reservation|gandiva|middle-ground>"
  exit 1
fi

# Use sed to update the file
sed -i.bak -E "s/^scheduling-policy:.*/scheduling-policy: $new_value/" "$target_file"

if [[ $? -eq 0 ]]; then
  echo "Successfully updated 'scheduling-policy' to '$new_value' in '$target_file'."
else
  echo "Error: Failed to update the file."
  exit 1
fi

#####################################
# Update Local Daemon configuration #
#####################################

# File to modify
target_file="./local_daemon/daemon.yml"

# Ensure the file exists
if [[ ! -f "$target_file" ]]; then
  echo "Error: File '$target_file' does not exist."
  exit 1
fi

# Ensure the argument is provided and valid
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <default|static|dynamic-v3|dynamic-v4|fcfs-batch|auto-scaling-fcfs-batch|reservation|gandiva|middle-ground>"
  exit 1
fi

# Get the first argument and validate
new_value=$1
if [[ "$new_value" != "default" && "$new_value" != "static" && "$new_value" != "dynamic-v3" && "$new_value" != "dynamic-v4" && "$new_value" != "fcfs-batch" && "$new_value" != "auto-scaling-fcfs-batch" && "$new_value" != "reservation" && "$new_value" != "gandiva" && "$new_value" != "middle-ground" ]]; then
  echo "Error: Argument must be 'true' or 'false'."
  exit 1
fi

# Use sed to update the file
sed -i.bak -E "s/^scheduling-policy:.*/scheduling-policy: $new_value/" "$target_file"

if [[ $? -eq 0 ]]; then
  echo "Successfully updated 'scheduling-policy' to '$new_value' in '$target_file'."
else
  echo "Error: Failed to update the file."
  exit 1
fi