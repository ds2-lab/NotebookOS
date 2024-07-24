#!/bin/bash

# This is an installation script to prepare an Ubuntu virtual machine for development.

mkdir ~/go
mkdir ~/go/pkg 

pushd ~/go/pkg 

if [ "$1" != "" ]; then
    GIT_TOKEN=$1
    git clone https://Scusemua@$(GIT_TOKEN)github.com/zhangjyr/distributed-notebook
else 
    git clone https://Scusemua@github.com/zhangjyr/distributed-notebook
fi 

popd 

# Python 3
if ! command python3.11 --version &> /dev/null; then 
    printf "\n[WARNING] Python3.11 is not installed. Installing it now...\n"

    sudo apt-get --assume-yes install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev

    cd /tmp/
    wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz
    tar -xf Python-3.11.9.tgz
    cd Python-3.11.9
    mkdir debug 
    cd debug 
    ../configure --enable-optimizations --with-pydebug --enable-shared 
    make -j$(nproc) EXTRA_CFLAGS="-DPy_REF_DEBUG" 
    sudo make altinstall

    if ! command python3.11 --version &> /dev/null; then 
        printf "\n[ERROR] Failed to install python3.11.\n"
        exit 
    else 
        echo "Successfully installed python3.11"
    fi 
fi 

# Python3 Pip
if ! command -v python3.11 -m pip &> /dev/null; then 
    printf "\n[WARNING] python3-pip is not installed. Installing it now."

    # sudo apt-get --assume-yes install python3-pip
    python3.11 -m ensurepip --upgrade 

    if ! command -v python3.11 -m pip &> /dev/null; then 
        printf "\n[ERROR] Installation of python3-pip failed."
        exit 
    fi
fi

# Git 
if ! command -v git version &> /dev/null; then 
    printf "\n[WARNING] git is not installed. Installing it now."

    sudo apt-get --assume-yes install git

    if ! command -v git version &> /dev/null; then 
        printf "\n[ERROR] Installation of git failed."
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

    echo Adding user $USER to 'docker' group.
    sudo usermod -aG docker $USER

    if ! command -v docker run hello-world &> /dev/null; then 
        printf "\n[ERROR] Failed to enable non-root user $USER to use Docker.\n"
        exit 
    else 
        echo "Successfully enabled non-root user $USER to use Docker.\n"
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
    sudo rm -rf /usr/local/go && tar -C /usr/local -xzf go$TARGET_GO_VERSION.linux-amd64.tar.gz
    export PATH=$PATH:/usr/local/go/bin

    echo export PATH=$PATH:/usr/local/go/bin >> $HOME/.profile

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

if ! command cd $GOPATH_ENV/pkg &> /dev/null; then 
    printf "\n[ERROR] Directory \"$GOPATH_ENV/pkg\" does not appear to exist...\n"
    echo "[WARNING] Attempting to create GOPATH directory \"$GOPATH_ENV/pkg\" now."
    if ! command mkdir -p $GOPATH_ENV/pkg &> /dev/null; then 
        echo "[ERROR] Failed to create GOPATH directory \"$GOPATH_ENV/pkg\""
        exit  
    fi 

    if ! command cd $GOPATH_ENV/pkg &> /dev/null; then 
        printf "\n[ERROR] Directory \"$GOPATH_ENV/pkg\" still does not appear to exist...\n"
        exit 
    fi 
fi 

# Kind 
go install sigs.k8s.io/kind@v0.22.0

# Protoc Golang Bindings 
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Python Proto Bindings
python3.11 -m pip install --user grpcio-tools
python3 -m pip install --user grpcio-tools

cd ~/go/pkg

if ! command stat zmq4 &> /dev/null; then 
    git clone https://github.com/go-zeromq/zmq4.git
fi 

if ! command stat gopy &> /dev/null; then 
    git clone https://github.com/Scusemua/gopy.git
fi 

if ! command stat distributed-notebook &> /dev/null; then 
    git clone https://github.com/zhangjyr/distributed-notebook.git
fi 

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
cd $GOPATH_ENV/pkg/gopy 
python3.11 -m pip install pybindgen
go install golang.org/x/tools/cmd/goimports@latest
go install github.com/scusemua/gopy@v0.4.3
make 
docker build -t scusemua/gopy .

cd $GOPATH_ENV/pkg/distributed-notebook 
cd smr && make build-linux-amd64
git checkout ben/feature/docker
make build-smr-linux-amd64
