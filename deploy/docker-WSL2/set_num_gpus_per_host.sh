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
  echo "Usage: $0 <true|false>"
  exit 1
fi

# Get the first argument and validate
new_value=$1
re='^[0-9]+$'
if ! [[ $new_value =~ $re ]] ; then
    echo "Error: Argument must be a positive integer."
    exit 1
fi

# Use sed to update the file
sed -i.bak -E "s/^gpus-per-host:.*/gpus-per-host: $new_value/" "$target_file"

if [[ $? -eq 0 ]]; then
  echo "Successfully updated 'gpus-per-host' to '$new_value' in '$target_file'."
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
  echo "Usage: $0 <true|false>"
  exit 1
fi

# Get the first argument and validate
new_value=$1
re='^[0-9]+$'
if ! [[ $new_value =~ $re ]] ; then
    echo "Error: Argument must be a positive integer."
    exit 1
fi

# Use sed to update the file
sed -i.bak -E "s/^gpus-per-host:.*/gpus-per-host: $new_value/" "$target_file"

if [[ $? -eq 0 ]]; then
  echo "Successfully updated 'gpus-per-host' to '$new_value' in '$target_file'."
else
  echo "Error: Failed to update the file."
  exit 1
fi
