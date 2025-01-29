#!/bin/bash

REQUIREMENTS_FILE="distributed_notebooks_requirements.txt"
NEW_TORCH_ENTRY="torch @ https://download.pytorch.org/whl/cpu-cxx11-abi/torch-2.5.1%2Bcpu.cxx11.abi-cp312-cp312-linux_x86_64.whl#sha256=0b55f1516410e4255132533b9f5a9621e48b7504d8adf22d927c57c9fa441bfd"

# Replace the line starting with "torch" with the CPU-only dependency
sed -i "s|^torch.*|$NEW_TORCH_ENTRY|" "$REQUIREMENTS_FILE"
