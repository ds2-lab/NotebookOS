# Deploying via Docker
This directory contains the files necessary to deploy the Distributed Jupyter Cluster using `docker compose`. 

Simply execute the following command:
``` sh
docker compose up -d --build --scale daemon=4`
```

You can replace the `"4"` passed to the `"--scale daemon="` argument with another value if you'd like to have a different number of Local Daemon containers. The recommended minimum is 4, as this allows for migrations to occur. 

## Development Recommendations

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