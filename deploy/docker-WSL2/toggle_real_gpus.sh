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
if [[ "$new_value" != "true" && "$new_value" != "false" ]]; then
  echo "Error: Argument must be 'true' or 'false'."
  exit 1
fi

old_image=""
new_image=""
if [[ "$new_value" == "true" ]]; then
  echo "Enabling real GPU support."
  echo ""

  old_image="scusemua/jupyter-cpu-dev:latest"
  new_image="scusemua/jupyter-gpu:latest"
else
  echo "Disabling real GPU support."
  echo ""

  old_image="scusemua/jupyter-gpu:latest"
  new_image="scusemua/jupyter-cpu-dev:latest"
fi

sed -i.bak -E "s~^notebook-image-name:.*~notebook-image-name: $new_image~" "$target_file"

if [[ $? -eq 0 ]]; then
  echo "Successfully updated 'notebook-image-name' parameter to '$new_image' in '$target_file'."
else
  echo ""
  echo "Error: Failed to update the 'notebook-image-name' parameter value."
  exit 1
fi

# Use sed to update the file
sed -i.bak -E "s/^use_real_gpus:.*/use_real_gpus: $new_value/" "$target_file"

if [[ $? -eq 0 ]]; then
  echo "Successfully updated 'use_real_gpus' to '$new_value' in '$target_file'."
  echo ""
else
  echo ""
  echo "Error: Failed to update the 'use_real_gpus' parameter value."
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

sed -i.bak -E "s~^notebook-image-name:.*~notebook-image-name: $new_image~" "$target_file"

if [[ $? -eq 0 ]]; then
  echo "Successfully updated 'notebook-image-name' parameter to '$new_image' in '$target_file'."
else
  echo ""
  echo "Error: Failed to update the 'notebook-image-name' parameter value."
  exit 1
fi

# Use sed to update the file
sed -i.bak -E "s/^use_real_gpus:.*/use_real_gpus: $new_value/" "$target_file"

if [[ $? -eq 0 ]]; then
  echo "Successfully updated 'use_real_gpus' to '$new_value' in '$target_file'."
  echo ""
else
  echo "Error: Failed to update the file."
  exit 1
fi

##################################
# Update docker-compose.yml file #
##################################

# File to modify
target_file="./docker-compose.yml"

# Ensure the file exists
if [[ ! -f "$target_file" ]]; then
  echo "Error: File '$target_file' does not exist."
  exit 1
fi

num_replacements=$(grep -o $old_image $target_file | wc -l)

# Use sed to update the file
sed -i.bak -E "s|$old_image.*|$new_image|" "$target_file"

if [[ $? -eq 0 ]]; then
  echo "Successfully changed all $num_replacements instance(s) of '$old_image' to '$new_image' in '$target_file'."
  echo ""
else
  echo "Error: Failed to update the file."
  exit 1
fi