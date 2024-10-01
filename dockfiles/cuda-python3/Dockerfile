ARG CUDA_VERSION
FROM nvidia/cuda:${CUDA_VERSION}

ENV LANG=C.UTF-8 \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC

# Install necessary packages
RUN apt-get update && apt-get install -y \
        software-properties-common \
        wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install python3.9
RUN add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y python3.9 python3.9-dev python3.9-distutils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    \
    && ln -sf /usr/bin/pydoc3.9 /usr/bin/pydoc3 \
    && ln -sf /usr/bin/python3.9 /usr/bin/python3 \
    && ln -s /usr/bin/python3.9-config /usr/bin/python3-config \
    && ln -s /usr/bin/pydoc3 /usr/bin/pydoc \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && ln -s /usr/bin/python3-config /usr/bin/python-config

# Install pip
RUN wget -q -O /tmp/get-pip.py https://bootstrap.pypa.io/get-pip.py \
    && /usr/bin/python3.9 /tmp/get-pip.py \
    && python3 -m pip install --upgrade pip setuptools wheel