# Deploying via Docker Compose
This directory contains the files necessary to deploy the Distributed Jupyter Cluster using `docker compose`. 

Simply execute the following command:
``` sh
docker compose up -d --build --scale daemon=4`
```

You can replace the `"4"` passed to the `"--scale daemon="` argument with another value if you'd like to have a different number of Local Daemon containers. The recommended minimum is 4, as this allows for migrations to occur. 

## Development Recommendations

### Core Dumps
The system is configured with the expectation that core dumps will be written to a `/cores` directory: [related](https://stackoverflow.com/questions/28335614/how-to-generate-core-file-in-docker-container).

### Dozzle
For debugging and development, we recommend deploying [Dozzle](https://github.com/amir20/dozzle) for easier monitoring of logs:
``` sh
docker run --name dozzle -d --volume=/var/run/docker.sock:/var/run/docker.sock:ro -p 7744:8080 amir20/dozzle:latest
```

This will expose a `Dozzle` log console at `localhost:7744`. 

### Watchtower
We also recommend deploying [Watchtower](https://github.com/containrrr/watchtower) to automatically redeploy containers, such as the Gateway or Local Daemons, when updates to their Docker images are made.

Watchtower can be deployed via:
``` sh
docker run -d --name watchtower -v /var/run/docker.sock:/var/run/docker.sock containrrr/watchtower
```

# Grafana Image Renderer

You can render the dashboard by issuing the HTTP requests of the following form:
```shell
http://localhost:3000/render/d/ddx4gnyl0cmbka/distributed-cluster?scale=4&height=3200&from=now-105m&to=now&timezone=browser&var-local_daemon_id=$__all&refresh=5s
```

## Configuration

The most important configuration files are `deploy/docker-WSL2/local_daemon/daemon.yml` and `deploy/docker-WSL2/gateway/gateway.yml`. In general, it's important that the configurations specified in these two files remain consistent.

We provide several scripts to (ever so slightly) simplify the task of updating certain values found in these configurations. These scripts are `set_scheduling_policy.sh` and `toggle_real_gpus.sh`.

### The `toggle_real_gpus.sh` Script
This script can be used to set the value of the `use_real_gpus` flag in the two aforementioned configuration files. To use the script, simply run it and pass either `true` or `false` as the first (and only) command line argument:
``` shell
./toggle_real_gpus.sh <true|false>
```

For example, to set the `use_real_gpus` flag to `false`, you would simply run the following:
``` shell 
./toggle_real_gpus.sh false 
```

### The `set_scheduling_policy.sh` Script
This script can be used to set the value of the `scheduling-policy` property in the two aforementioned configuration files. To use the script, simply run it and pass one of the valid scheduling policies as the first (and only) command line argument:
``` shell
./set_scheduling_policy.sh <default|static|dynamic-v3|dynamic-v4|fcfs-batch|auto-scaling-fcfs-batch|reservation|gandiva>
```

For example, you can set the `scheduling-policy` property to `static` by running the following:
``` shell 
./set_scheduling_policy.sh static 
```