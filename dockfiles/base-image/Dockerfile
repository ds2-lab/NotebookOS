# FROM debian:bullseye:latest
FROM debian:bookworm

ARG USER_ID
ARG GROUP_ID

RUN addgroup --gid $GROUP_ID hostuser
RUN adduser --disabled-password --gecos '' --uid $USER_ID --gid $GROUP_ID hostuser

RUN apt-get update && apt-get -y install ca-certificates openssh-client wget

# ensure that nsswitch.conf is set up for Go's "netgo" implementation (which Docker explicitly uses)
# - https://github.com/moby/moby/blob/v24.0.6/hack/make.sh#L111
# - https://github.com/golang/go/blob/go1.19.13/src/net/conf.go#L227-L303
# - docker run --rm debian:stretch grep '^hosts:' /etc/nsswitch.conf
# RUN [ -e /etc/nsswitch.conf ] && grep '^hosts: files dns' /etc/nsswitch.conf

ENV DOCKER_VERSION=24.0.7

RUN set -eux; \
	\
	dpkgArch="$(dpkg --print-architecture)"; \
	case "$dpkgArch" in \
	'amd64') \
	url='https://download.docker.com/linux/static/stable/x86_64/docker-24.0.7.tgz'; \
	;; \
	'armhf') \
	url='https://download.docker.com/linux/static/stable/armel/docker-24.0.7.tgz'; \
	;; \
	'armv7') \
	url='https://download.docker.com/linux/static/stable/armhf/docker-24.0.7.tgz'; \
	;; \
	'aarch64') \
	url='https://download.docker.com/linux/static/stable/aarch64/docker-24.0.7.tgz'; \
	;; \
	*) echo >&2 "error: unsupported 'docker.tgz' architecture ($dpkgArch)"; exit 1 ;; \
	esac; \
	\
	wget -O 'docker.tgz' "$url"; \
	\
	tar --extract \
	--file docker.tgz \
	--strip-components 1 \
	--directory /usr/local/bin/ \
	--no-same-owner \
	'docker/docker' \
	; \
	rm docker.tgz; \
	\
	docker --version

ENV DOCKER_BUILDX_VERSION=0.12.0
RUN set -eux; \
	\
	dpkgArch="$(dpkg --print-architecture)"; \
	case "$dpkgArch" in \
	'amd64') \
	url='https://github.com/docker/buildx/releases/download/v0.12.0/buildx-v0.12.0.linux-amd64'; \
	sha256='7c393b92c148a0ce26c76a2abc99960be1d1097f0471978d41dc51d0c1a4471e'; \
	;; \
	'armhf') \
	url='https://github.com/docker/buildx/releases/download/v0.12.0/buildx-v0.12.0.linux-arm-v6'; \
	sha256='62d9162f526c3bb7f67768a5b0d81a8b3ad0371406dfc1f0775e4f62dfca7fe1'; \
	;; \
	'armv7') \
	url='https://github.com/docker/buildx/releases/download/v0.12.0/buildx-v0.12.0.linux-arm-v7'; \
	sha256='d941d6a5b072de775222d31d9f8467b4c1b1f56e3b08d1b78f828a9244c16464'; \
	;; \
	'aarch64') \
	url='https://github.com/docker/buildx/releases/download/v0.12.0/buildx-v0.12.0.linux-arm64'; \
	sha256='781caebb36551b035cb9dcfaf91088543d09c73c4a2549341e6417d86b8bbb50'; \
	;; \
	'ppc64le') \
	url='https://github.com/docker/buildx/releases/download/v0.12.0/buildx-v0.12.0.linux-ppc64le'; \
	sha256='ab5bda4532528d6b0801c877999fce9def10c6a37624673fd13c668fdcde16b7'; \
	;; \
	'riscv64') \
	url='https://github.com/docker/buildx/releases/download/v0.12.0/buildx-v0.12.0.linux-riscv64'; \
	sha256='a2b846919c44128c6db9165ad24545e7e10035b6f0ad01559fcfbb2a13017127'; \
	;; \
	's390x') \
	url='https://github.com/docker/buildx/releases/download/v0.12.0/buildx-v0.12.0.linux-s390x'; \
	sha256='81c2ada65624e2ac6bb4123f3a3bb933d04cfb08aa45fc55dd201ba523d96d30'; \
	;; \
	*) echo >&2 "warning: unsupported 'docker-buildx' architecture ($dpkgArch); skipping"; exit 0 ;; \
	esac; \
	\
	wget -O 'docker-buildx' "$url"; \
	echo "$sha256 *"'docker-buildx' | sha256sum -c -; \
	\
	plugin='/usr/local/libexec/docker/cli-plugins/docker-buildx'; \
	mkdir -p "$(dirname "$plugin")"; \
	mv -vT 'docker-buildx' "$plugin"; \
	chmod +x "$plugin"; \
	\
	docker buildx version

ENV DOCKER_COMPOSE_VERSION=2.23.3
RUN set -eux; \
	\
	dpkgArch="$(dpkg --print-architecture)"; \
	case "$dpkgArch" in \
	'amd64') \
	url='https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-linux-x86_64'; \
	sha256='a836e807951db448f991f303cddcc9a4ec69f4a49d58bc7d536cb91c77c04c33'; \
	;; \
	'armhf') \
	url='https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-linux-armv6'; \
	sha256='b712693945360155842b578beace00effa723b604bfe1ccd6421645523e15d86'; \
	;; \
	'armv7') \
	url='https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-linux-armv7'; \
	sha256='4068bcbe1dd90034c8fe8d2c65b600ba793fc19bdb65db3c2dbf80b8a078de6c'; \
	;; \
	'aarch64') \
	url='https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-linux-aarch64'; \
	sha256='71f38f0923b8a9b80ad02c823ec3207d94677547aa5c618ca41b81d29fe6b9d9'; \
	;; \
	'ppc64le') \
	url='https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-linux-ppc64le'; \
	sha256='6110b0d30baee103c98ca5503bea24acb9d52bd333a67d3bf3c57d383c585c62'; \
	;; \
	'riscv64') \
	url='https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-linux-riscv64'; \
	sha256='3ac26e5f272deb1364c9b8760af44c4dbd87d6faa42fc53bfec95885cfa8ae77'; \
	;; \
	's390x') \
	url='https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-linux-s390x'; \
	sha256='2886dd4eddaea1eeb03537bdc596ec8947eb3ef7908c955284f8aad9170d3098'; \
	;; \
	*) echo >&2 "warning: unsupported 'docker-compose' architecture ($dpkgArch); skipping"; exit 0 ;; \
	esac; \
	\
	wget -O 'docker-compose' "$url"; \
	echo "$sha256 *"'docker-compose' | sha256sum -c -; \
	\
	plugin='/usr/local/libexec/docker/cli-plugins/docker-compose'; \
	mkdir -p "$(dirname "$plugin")"; \
	mv -vT 'docker-compose' "$plugin"; \
	chmod +x "$plugin"; \
	\
	ln -sv "$plugin" /usr/local/bin/; \
	docker-compose --version; \
	docker compose version

COPY modprobe.sh /usr/local/bin/modprobe
COPY docker-entrypoint.sh /usr/local/bin/

# https://github.com/docker-library/docker/pull/166
#   dockerd-entrypoint.sh uses DOCKER_TLS_CERTDIR for auto-generating TLS certificates
#   docker-entrypoint.sh uses DOCKER_TLS_CERTDIR for auto-setting DOCKER_TLS_VERIFY and DOCKER_CERT_PATH
# (For this to work, at least the "client" subdirectory of this path needs to be shared between the client and server containers via a volume, "docker cp", or other means of data sharing.)
ENV DOCKER_TLS_CERTDIR=/certs
# also, ensure the directory pre-exists and has wide enough permissions for "dockerd-entrypoint.sh" to create subdirectories, even when run in "rootless" mode
RUN mkdir /certs /certs/client && chmod 1777 /certs /certs/client
# (doing both /certs and /certs/client so that if Docker does a "copy-up" into a volume defined on /certs/client, it will "do the right thing" by default in a way that still works for rootless users)

# https://github.com/docker/docker/blob/master/project/PACKAGERS.md#runtime-dependencies
RUN set -eux; \
	apt-get -y install \
	btrfs-progs \
	e2fsprogs \
	# e2fsprogs-extra \
	# ip6tables \
	iptables \
	openssl \
	# shadow \
	make \
	uidmap \
	xfsprogs \
	xz-utils \
	# pigz: https://github.com/moby/moby/pull/35697 (faster gzip implementation)
	pigz \
	; \
	# only install zfs if it's available for the current architecture
	# https://git.alpinelinux.org/cgit/aports/tree/main/zfs/APKBUILD?h=3.6-stable#n9 ("all !armhf !ppc64le" as of 2017-11-01)
	# "apk info XYZ" exits with a zero exit code but no output when the package exists but not for this arch
	if zfs="$(apt show --quiet zfs)" && [ -n "$zfs" ]; then \
	apt-get -y install zfs; \
	fi

# TODO aufs-tools

# set up subuid/subgid so that "--userns-remap=default" works out-of-the-box
RUN set -eux;
RUN addgroup --system dockremap; 
RUN useradd -g dockremap dockremap;
# RUN adduser --system dockremap dockremap; 
RUN echo 'dockremap:165536:65536' >> /etc/subuid; 
RUN echo 'dockremap:165536:65536' >> /etc/subgid

RUN set -eux; \
	\
	dpkgArch="$(dpkg --print-architecture)"; \
	case "$dpkgArch" in \
	'amd64') \
	url='https://download.docker.com/linux/static/stable/x86_64/docker-24.0.7.tgz'; \
	;; \
	'armhf') \
	url='https://download.docker.com/linux/static/stable/armel/docker-24.0.7.tgz'; \
	;; \
	'armv7') \
	url='https://download.docker.com/linux/static/stable/armhf/docker-24.0.7.tgz'; \
	;; \
	'aarch64') \
	url='https://download.docker.com/linux/static/stable/aarch64/docker-24.0.7.tgz'; \
	;; \
	*) echo >&2 "error: unsupported 'docker.tgz' architecture ($dpkgArch)"; exit 1 ;; \
	esac; \
	\
	wget -O 'docker.tgz' "$url"; \
	\
	tar --extract \
	--file docker.tgz \
	--strip-components 1 \
	--directory /usr/local/bin/ \
	--no-same-owner \
	# we exclude the CLI binary because we already extracted that over in the "docker:24-cli" image that we're FROM and we don't want to duplicate those bytes again in this layer
	--exclude 'docker/docker' \
	; \
	rm docker.tgz; \
	\
	dockerd --version; \
	containerd --version; \
	ctr --version; \
	runc --version

# https://github.com/docker/docker/tree/master/hack/dind
ENV DIND_COMMIT=65cfcc28ab37cb75e1560e4b4738719c07c6618e

RUN set -eux; \
	wget -O /usr/local/bin/dind "https://raw.githubusercontent.com/docker/docker/${DIND_COMMIT}/hack/dind"; \
	chmod +x /usr/local/bin/dind

COPY dockerd-entrypoint.sh /usr/local/bin/

RUN cd /tmp && wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz && rm -rf /usr/local/go && tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin

VOLUME /var/lib/docker
EXPOSE 2375 2376

# For debugging.
RUN apt-get update && apt-get install iputils-ping dnsutils vim -y 

USER hostuser

ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD []