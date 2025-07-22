#!/bin/bash

# This is an installation script to prepare an Ubuntu virtual machine for development.
sudo apt-get --assume-yes install unzip make

# Git
if ! command -v git version &> /dev/null; then
    printf "\n[WARNING] git is not installed. Installing it now."

    sudo apt-get --assume-yes install git

    if ! command -v git version &> /dev/null; then
        printf "\n[ERROR] Installation of git failed."
        exit
    fi
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
PYTHON_VERSION=3.12.6
PYTHON_MAJOR_VERSION=3.12

mkdir ~/go
mkdir ~/go/pkg 

pushd ~/go/pkg

sudo apt-get update

if [ "$1" != "" ]; then
    GIT_TOKEN=$1
    git clone https://github.com/ds2-lab/NotebookOS/
else 
    git clone https://github.com/ds2-lab/NotebookOS/
fi 

popd

# Ansible
if ! command ansible --version &> /dev/null; then
  sudo apt-get install --assume-yes ansible
fi

check_python_version() {
    # Try different python commands
    for cmd in python3.12 python3 python; do
        if command -v $cmd >/dev/null 2>&1; then
            version=$($cmd --version 2>&1 | awk '{print $2}')
            if [ "$version" = $PYTHON_VERSION ]; then
                echo "Python $PYTHON_VERSION is installed as $cmd"
                return 0
            fi
        fi
    done
    return 1
}

# Python 3
# if ! command python3 --version &> /dev/null; then
if check_python_version; then
    echo "Python $PYTHON_VERSION is already installed."
else
    printf "\n[WARNING] Python%s is not installed. Installing it now...\n" $PYTHON_VERSION
    printf "\n[WARNING] Python%s is not installed. Installing it now...\n" $PYTHON_VERSION

    sudo apt-get update
    
    sudo apt-get --assume-yes install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev libgdbm-dev libgdbm-compat-dev uuid-dev lzma lzma-dev liblzma-dev

    cd /tmp/
    wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
    tar -xf Python-$PYTHON_VERSION.tgz
    cd Python-$PYTHON_VERSION
    mkdir debug
    cd debug

    # ../configure --enable-optimizations --with-pydebug --enable-shared --with-ensurepip=install && make -j$(nproc) EXTRA_CFLAGS="-DPy_REF_DEBUG" && sudo make altinstall
    ../configure --enable-optimizations --with-pydebug --enable-shared --with-ensurepip=install
    make -j$(nproc) EXTRA_CFLAGS="-DPy_REF_DEBUG"
    sudo make altinstall
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/
    echo export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/ >> $HOME/.bashrc

    # if ! command python$PYTHON_MAJOR_VERSION --version &> /dev/null; then
    if check_python_version; then
        echo "Successfully installed Python-$PYTHON_VERSION"
    else 
        printf "\n[ERROR] Failed to install python%s.\n" $PYTHON_VERSION
        exit 
    fi 
fi 

# Python3 Pip
if ! command -v python$PYTHON_MAJOR_VERSION -m pip &> /dev/null; then
    printf "\n[WARNING] python3-pip is not installed. Installing it now."

    # sudo apt-get --assume-yes install python3-pip
    python$PYTHON_MAJOR_VERSION -m ensurepip --upgrade

    if ! command -v python$PYTHON_MAJOR_VERSION -m pip &> /dev/null; then
        printf "\n[ERROR] Installation of python3-pip failed."
        exit 
    fi
fi

# Docker 
if ! command -v docker version &> /dev/null; then 
    printf "\n[WARNING] Docker is not installed. Installing Docker now."

    # Add Docker's official GPG key:
    sudo apt-get update
    sudo apt-get --assume-yes install ca-certificates curl
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc

    echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update

    sudo apt-get --assume-yes install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

    if ! command -v sudo docker run hello-world &> /dev/null; then 
        printf "\n[ERROR] Docker installation failed.\n"
        exit 
    else 
        echo "Successfully installed Docker."
    fi 
fi 

# Protoc (protobuffers compiler)
if ! command protoc --version &> /dev/null; then 
    pushd /tmp 

    PB_REL="https://github.com/protocolbuffers/protobuf/releases"
            
    curl -LO $PB_REL/download/v27.2/protoc-27.2-linux-x86_64.zip
    unzip protoc-27.2-linux-x86_64.zip -d $HOME/.local
    export PATH="$PATH:$HOME/.local/bin"

    popd 
fi

# Docker, non-root user.
if ! command -v docker run hello-world &> /dev/null; then 
    printf "\n[WARNING] Failed to enable non-root user to use Docker. Enabling non-root user to use Docker now...\n"

    if ! command grep -q docker /etc/group; then 
        echo Creating 'docker' group.
        sudo groupadd docker
    fi 

    printf "Adding user %s to 'docker' group." "$USER"
    sudo usermod -aG docker $USER
    newgrp docker

    if ! command -v docker run hello-world &> /dev/null; then 
        printf "\n[ERROR] Failed to enable non-root user %s to use Docker.\n" "$USER"
        exit 
    else 
        printf "Successfully enabled non-root user %s to use Docker.\n" "$USER"
    fi 
else 
    echo "Non-root user $USER can use Docker!"
fi 

# Kubectl 
if ! command -v kubectl version &> /dev/null; then 
    printf "\n[WARNING] kubectl is not installed. Installing now.\n"

    cd /tmp
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl.sha256"

    OUTPUT=$(echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check)
    if [ "$OUTPUT" != "kubectl: OK" ]; then
        echo \n"[ERROR] Failed to install kubectl. Installed binary did not validate."
        exit 1 
    else 
        sudo chmod +x kubectl
        sudo mv kubectl /usr/bin/kubectl
        echo "Successfully installed kubectl."
    fi 
fi

# Helm 
if ! command -v helm version &> /dev/null; then 
    printf "\n[WARNING] helm is not installed. Installing now.\n"

    cd /tmp
    curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
    chmod 700 get_helm.sh
    /bin/bash ./get_helm.sh

    if ! command -v helm version &> /dev/null; then 
        printf "\n[ERROR] Helm installation failed.\n"
        exit 
    else 
        echo "Successfully installed Helm."
    fi 
fi 

##########
# Golang #
##########
if ! command -v go version &> /dev/null
then 
    printf "\n[WARNING] Golang not installed. Attempting to install it now...\n"

    TARGET_GO_VERSION=1.21.5
    GO_URL=https://go.dev/dl/go$TARGET_GO_VERSION.linux-amd64.tar.gz
    echo "[DEBUG] Downloading Golang v$TARGET_GO_VERSION from $GO_URL"
    cd /tmp && wget $GO_URL
    echo "[DEBUG] Downloaded Golang v$TARGET_GO_VERSION from $GO_URL. Installing now..."
    sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go$TARGET_GO_VERSION.linux-amd64.tar.gz
    export PATH=$PATH:/usr/local/go/bin

    echo export PATH=$PATH:/usr/local/go/bin >> $HOME/.profile
    echo export PATH=$PATH:/usr/local/go/bin >> $HOME/.bashrc

    echo "[DEBUG] Checking if Golang installed successfully..."

    OUTPUT=$(go version)

    if [ "$OUTPUT" != "go version go$TARGET_GO_VERSION linux/amd64" ]; then
        printf "\n[ERROR] Golang installation failed.\n"
        echo "[ERROR] Unexpected Golang version installed: $OUTPUT"
        exit 
    fi 

    echo "Golang v$TARGET_GO_VERSION installed successfully."
fi

GOPATH_ENV=$(go env GOPATH)

echo "\$GOPATH is set to \"$GOPATH_ENV\""

if ! command cd "$GOPATH_ENV/pkg" &> /dev/null; then
    printf "\n[ERROR] Directory '%s/pkg' does not appear to exist...\n" "$GOPATH_ENV"
    printf "[WARNING] Attempting to create GOPATH directory '%s/pkg' now." "$GOPATH_ENV"
    if ! command mkdir -p "$GOPATH_ENV/pkg" &> /dev/null; then
        echo "[ERROR] Failed to create GOPATH directory '%s/pkg'" "$GOPATH_ENV"
        exit  
    fi 

    if ! command cd "$GOPATH_ENV/pkg" &> /dev/null; then
        printf "\n[ERROR] Directory '%s/pkg' still does not appear to exist...\n" "$GOPATH_ENV"
        exit 
    fi 
fi 

# Kind 
go install sigs.k8s.io/kind@v0.22.0

# Protoc Golang Bindings 
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

# Python Proto Bindings
python3.12 -m pip install --user grpcio-tools
python3 -m pip install --user grpcio-tools

pushd ~/go/pkg

if ! command stat zmq4 &> /dev/null; then 
    git clone https://github.com/go-zeromq/zmq4.git
fi 

if ! command stat gopy &> /dev/null; then 
    git clone https://github.com/Scusemua/gopy.git
fi 

if ! command stat NotebookOS &> /dev/null; then 
    git clone https://github.com/ds2-lab/NotebookOS
fi 

popd

###############
# Hadoop HDFS #
###############

# Install Java
sudo apt-get update && sudo apt-get install -y openjdk-8-jdk ssh

# Create hadoop user
HADOOP_USER=hadoop
HADOOP_PASSWORD="12345"
sudo useradd -p "$(openssl passwd -6 $HADOOP_PASSWORD)" $HADOOP_USER

sudo mkdir /home/hadoop/
sudo mkdir /home/hadoop/.ssh
sudo chmod -R 777 /home/hadoop/
sudo chown -R hadoop /home/hadoop/

# Create SSH key for hadoop and move it to the proper location.
pushd ~/.ssh/
ssh-keygen -t rsa -N "" -f hadoop.key
cp hadoop.key /home/hadoop/.ssh/hadoop.key 
cp hadoop.key.pub /home/hadoop/.ssh/hadoop.key.pub
cp hadoop.key /home/hadoop/.ssh/id_rsa
cp hadoop.key.pub /home/hadoop/.ssh/id_rsa.pub
cat ~/.ssh/hadoop.key.pub >> ~/.ssh/authorized_keys
sudo cp ~/.ssh/authorized_keys /home/hadoop/.ssh/authorized_keys
chmod 640 ~/.ssh/authorized_keys
sudo chown hadoop /home/hadoop/.ssh/authorized_keys
sudo chown hadoop /home/hadoop/.ssh/hadoop.key 
sudo chown hadoop /home/hadoop/.ssh/hadoop.key.pub
sudo chown hadoop /home/hadoop/.ssh/id_rsa
sudo chown hadoop /home/hadoop/.ssh/id_rsa.pub
popd 

# Download hadoop HDFS and set it up in the hadoop user's home directory.
# NOTE: you may have to execute these commands manually, one at a time, if this doesn't work... 
ssh -o StrictHostKeyChecking=accept-new -i ~/.ssh/hadoop.key hadoop@localhost "wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz ; tar -xvzf hadoop-3.3.6.tar.gz ; mv hadoop-3.3.6 hadoop ; mkdir -p ~/hadoopdata/hdfs/{namenode,datanode}"

pushd "$ROOT_DIR/setup"

# Set some environment variables in the hadoop user's .bashrc file as well as the hadoop-env.sh file (only the latter of which is more important/actually does something...)
sudo bash -c 'cat hadoop-env >> /home/hadoop/.bashrc'
sudo bash -c 'cat hadoop-env >> /home/hadoop/hadoop/etc/hadoop/hadoop-env.sh'

# Copy the HDFS configuration.
sudo cp -r ./hdfs_configuration/* /home/hadoop/hadoop/etc/hadoop/
sudo chown -R hadoop /home/hadoop/hadoop/etc/hadoop/

# Format the file system.
ssh -i ~/.ssh/hadoop.key hadoop@localhost 'env JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 ~/hadoop/bin/hdfs namenode -format'

#################
# scusemua/gopy #
#################
pushd "$GOPATH_ENV/pkg/gopy"
python3.12 -m pip install pybindgen
go install golang.org/x/tools/cmd/goimports@latest
make 
docker build -t $DOCKERUSER/gopy .
popd

pushd "$GOPATH_ENV/pkg/NotebookOS"
pushd smr
make build-linux-amd64
popd
make build-smr-linux-amd64
