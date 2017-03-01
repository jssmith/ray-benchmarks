# Ray Benchmarks and Stress Tests


## Install Ray with Docker

[Install Docker and build the Ray Docker images](https://github.com/ray-project/ray/blob/master/doc/source/install-on-docker.md).

Confirm that the image `ray-project/deploy` is present by running the command:

```
docker images
```


## Build the Benchmark Docker image

Run the command:

```
./build-docker.sh
```

## Run 

```
python stress/matrix.py config/test.json
```


## Tips

### Run workload script

Workload scripts should be designed to run from the command line as well as from within the benchmark environment.
Try running one with the following command:

```
python workload_scripts/wc.py
```

### Iterate an individual workload

The loop script runs workloads iteratively and allows for straightforward configuration of the Ray system.
Try it with the following example:

```
python stress/loop.py \
    --workload=workloads/wc.py \
    --num-workers=10 \
    --num-nodes=2 \
    --shm-size=1G \
    --iteration-target=3
```

### Managing Docker

The loop script starts Docker containers and contains logic to kill them if they are running for too long.
Still, you may find yourself wanting to manually inspect or kill Docker containers.

To see what Docker container are running use:

```
docker ps
```
Note the container id of interest and use it in place of `<container-id>` in the commands that follow.

To open a shell within a container for debugging use:
```
docker exec -t -i <container-id>
```

To kill a single Docker container use:
```
docker kill <container-id>
```

To kill all running docker containers use:
```
docker kill $(docker ps -q)
```
