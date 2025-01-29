#!/bin/bash

REQUIREMENTS_FILE="distributed_notebooks_requirements.txt"

# torch (PyTorch)
NEW_TORCH_ENTRY="torch @ https://download.pytorch.org/whl/cpu-cxx11-abi/torch-2.5.1%2Bcpu.cxx11.abi-cp312-cp312-linux_x86_64.whl#sha256=0b55f1516410e4255132533b9f5a9621e48b7504d8adf22d927c57c9fa441bfd"
if grep -q "^torch$" "$REQUIREMENTS_FILE"; then
  sed -E -i "s|^torch\b.*|$NEW_TORCH_ENTRY|" "$REQUIREMENTS_FILE"
  echo "Updated 'torch' dependency entry"
else
  echo "No 'torch' entry in requirements file..."
fi

# torchvision
NEW_TORCHVISION_ENTRY="torchvision @ https://download.pytorch.org/whl/cpu/torchvision-0.20.1%2Bcpu-cp312-cp312-linux_x86_64.whl#sha256=5f46c7ac7f00a065cb40bfb1e1bfc4ba16a35f5d46b3fe70cca6b3cea7f822f7"
if grep -q "^torchvision$" "$REQUIREMENTS_FILE"; then
  sed -E -i "s|^torchvision\b.*|$NEW_TORCHVISION_ENTRY|" "$REQUIREMENTS_FILE"
  echo "Updated 'torchvision' dependency entry"
else
  echo "No 'torchvision' entry in requirements file..."
fi

# torchaudio
NEW_TORCHAUDIO_ENTRY="torchaudio @ https://download.pytorch.org/whl/cpu/torchaudio-2.5.1%2Bcpu-cp312-cp312-linux_x86_64.whl#sha256=b7fbd9c264dcbe28efb061364c76d3770eb13ae692d2982949b583edfb9ed7f5"
if grep -q "^torchaudio$" "$REQUIREMENTS_FILE"; then
  sed -E -i "s|^torchaudio\b.*|$NEW_TORCHAUDIO_ENTRY|" "$REQUIREMENTS_FILE"
  echo "Updated 'torchaudio' dependency entry"
else
  echo "No 'torchaudio' entry in requirements file..."
fi

# torchdata
NEW_TORCHDATA_ENTRY="torchdata @ https://download.pytorch.org/whl/cpu/torchdata-0.9.0%2Bcpu-cp312-cp312-linux_x86_64.whl#sha256=d12345f5692eed382af9ad1cd63ea09183f21b333cdc3f7c36f53ac93649f060"
if grep -q "^torchdata==0.9.0$" "$REQUIREMENTS_FILE"; then
  sed -E -i "s|^torchdata==0.9.0\b.*|$NEW_TORCHDATA_ENTRY|" "$REQUIREMENTS_FILE"
  echo "Updated 'torchdata' dependency entry"
else
  echo "No 'torchdata' entry in requirements file..."
fi