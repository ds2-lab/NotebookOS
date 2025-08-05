# AWS EC2 Installation Instructions
The following document provides instructions for manually setting up the environment required to deploy and run NotebookOS on an AWS EC2 virtual machine.

Special "thank you" to the ASPLOS'26 Artifact Evaluation reviewers for helping to improve the setup/installation documentation and instructions.

## Prerequisites

### (1) Install Golang
**(1a) Download the Latest Go (Stable Version)**
``` bash
wget https://go.dev/dl/go1.22.2.linux-amd64.tar.gz
```

**(1b) Extract to `/usr/local`**
``` bash
sudo tar -C /usr/local -xzf go1.22.2.linux-amd64.tar.gz
```

**(1c) Add Go environments (e.g., `vim ~/.bashrc`)**
``` bash
export PATH=$PATH:/usr/local/go/bin
export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin
```

**(1d) Update environments**
``` bash
source ~/.bashrc
```

**(1e) Verify installation**
``` bash 
go version
```

**All Commands Together**
``` bash
# 1. Download the Latest Go (Stable Version)
wget https://go.dev/dl/go1.22.2.linux-amd64.tar.gz

# 2. extract to `/usr/local`
sudo tar -C /usr/local -xzf go1.22.2.linux-amd64.tar.gz

# 3. Add Go environments (e.g., `vim ~/.bashrc`)
# Go environment
export PATH=$PATH:/usr/local/go/bin
export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin

# 4. update environments
source ~/.bashrc

# 5. verify
go version
```

## (2) Install Docker
**(2a) Update the system**
``` bash
sudo apt-get update
sudo apt-get upgrade -y
```

**(2b) Install prerequisite packages**
``` bash
sudo apt-get install -y \
 ca-certificates \
curl \
 gnupg \
 lsb-release
 ```

**(2c) Add Docker’s official GPG key**
``` bash
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
``` 

**(2e) Set up the Docker repository**
``` bash
echo \
"deb [arch=$(dpkg --print-architecture) \
 signed-by=/etc/apt/keyrings/docker.gpg] \
 https://download.docker.com/linux/ubuntu \
$(lsb_release -cs) stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
``` 

**(2f) Install Docker Engine**
``` bash
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin dockercompose-plugin
``` 

**(2g) Verify Docker is working**
``` bash
sudo docker version
sudo docker run hello-world
```

**(2h) Enable running Docker without `sudo`**
``` bash
sudo usermod -aG docker $USER
newgrp docker
```

**(2i) Verify that Docker can be run without `sudo`**
``` bash
docker run hello-world
```

**(2j) Docker login with username and credentials**
``` bash
docker login
```

**(2k) Get Docker username**
```
docker info | grep Username
```

**(2l) Set the `DOCKERUSER` environment variable in ~/.bashrc**
``` bash
export DOCKERUSER=<your docker username>
echo "export DOCKERUSER=<your docker username>" >> ~/.bashrc
```

**All Commands Together**
``` bash
# 1. update system
sudo apt-get update
sudo apt-get upgrade -y

# 2. Install prerequisite packages
sudo apt-get install -y \
 ca-certificates \
curl \
 gnupg \
 lsb-release

# 3. Add Docker’s official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# 4. Set up the Docker repository
echo \
"deb [arch=$(dpkg --print-architecture) \
 signed-by=/etc/apt/keyrings/docker.gpg] \
 https://download.docker.com/linux/ubuntu \
$(lsb_release -cs) stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 5. Install Docker Engine
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin dockercompose-plugin

# 6. Verify Docker is working
sudo docker version
sudo docker run hello-world

# 7. Run Docker without `sudo`
sudo usermod -aG docker $USER
newgrp docker

# 8. Verify docker
docker run hello-world

# 9. docker login with username and credentials
docker login

# 10. get docker username
docker info|grep Username

# 11. set Docker environment variable in ~/.bashrc, remember to source
export DOCKERUSER=<your docker username>
echo "export DOCKERUSER=<your docker username>" >> ~/.bashrc
```

### (3) Install `protoc` (Google Protobuffers Compiler) as well as `unzip`

**(3a) Install `unzip` if it is not already installed**
``` bash
sudo apt update
sudo apt install -y unzip
```

**(3b) Install `protoc`**
``` bash
# Define version
PROTOC_VERSION=27.2

# Download precompiled binary
curl -LO
https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-
${PROTOC_VERSION}-linux-x86_64.zip

# Unzip into /usr/local (requires sudo)
sudo unzip -o protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local

# Clean up
rm protoc-${PROTOC_VERSION}-linux-x86_64.zip

# verify
protoc --version

# if `$ which protoc` returns, but `--version` failed: clear Bash's command cache; then
re-verify
hash -r
```

## (4) Download NotebookOS

```  bash
# (4a) Create an example Golang project and init `~/go/pkg` directory.
# You can manually create it, but be aware of the permissions of hte home directory (~/).
mkdir test
cd test
go mod init example.com/test
go get github.com/google/uuid

# (4b) Clean the test project.
rm -rf test

# (4c) cd to ~/go/pkg
cd ~/go/pkg

# (4d) Download the source code.
git clone https://github.com/ds2-lab/NotebookOS.git

# (4e) Go to NotebookOS/setup/, run `install.sh` (before `hadoop` installation, everything should work)
./install.sh

# (4f) Small fix in case the script fails to set the ROOT_DIR environment variable. Get the correct value and set it in ~/.bashrc
export ROOT_DIR=/home/ubuntu/go/pkg/NotebookOS
echo "export ROOT_DIR=/home/ubuntu/go/pkg/NotebookOS" >> ~/.bashrc
```
