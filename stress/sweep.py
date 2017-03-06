import argparse
import json
import os
import time

from loop import Logger, StressRay

from subprocess32 import call, Popen, PIPE

def run_experiment(experiment_name, workload, system_config, log_directory):
    log_filename = os.path.join(log_directory, "{}_{}_{}_{}.log".format(
        workload["name"],
        system_config["num_nodes"],
        system_config["num_workers"],
        system_config["shm_size"]))

    print log_filename
    with Logger(log_filename) as logger:
        logger.log("experiment", {
            "experiment_name" : experiment_name,
            "system_config" : system_config,
            "workload" : workload
            })
        s = StressRay(logger)
        s.start_ray(shm_size=system_config["shm_size"], mem_size=system_config["mem_size"], num_workers=system_config["num_workers"], num_nodes=system_config["num_nodes"])
        time.sleep(2)
        execution_time_limit = workload["execution_time_limit"] if "execution_time_limit" in workload else None
        sequential_failures_limit = workload["sequential_failures_limit"] if "sequential_failures_limit" in workload else 3
        s.iterate_workload(workload["workload_script"], iteration_target=workload["iteration_target"], time_target=workload["time_target"], execution_time_limit=execution_time_limit, sequential_failures_limit=sequential_failures_limit)
        s.stop_ray()

def do_sweep(experiment_name, config, base_log_directory):
    log_directory = os.path.join(base_log_directory, experiment_name, time.strftime("%Y%m%d_%H%M%S"))
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    print("Output to {}".format(log_directory))

    for system_config in config["system_configs"]:
        for workload in config["workloads"]:
            run_experiment(experiment_name, workload, system_config, log_directory)


def get_rev(refname="HEAD", dir="."):
    proc = Popen(["git", "rev-parse", refname], cwd=dir, stdout=PIPE)
    (stdout, stderr) = proc.communicate()
    return stdout.strip()

def get_relationship(refname, other_refname):
    proc = Popen(["git", "rev-list", "--left-right", "--count", refname + "..." + other_refname], stdout=PIPE)
    (stdout, stderr) = proc.communicate()
    (ahead, behind) = map(lambda x: int(x), stdout.strip().split())
    return (ahead, behind)


def get_relationship(refname, other_refname, dir="."):
    proc = Popen(["git", "rev-list", "--left-right", "--count", refname + "..." + other_refname], cwd=dir, stdout=PIPE)
    (stdout, stderr) = proc.communicate()
    (ahead, behind) = map(lambda x: int(x), stdout.strip().split())
    return (ahead, behind)


def git_fetch(dir="."):
    Popen(["git", "fetch"], cwd=dir).wait()

def git_pull(dir="."):
    Popen(["git", "pull"], cwd=dir).wait()


def build_ray_docker(dir):
    proc = Popen(["/bin/bash", "build-docker.sh", "--skip-examples"], cwd=dir)
    proc.wait()
    return proc.returncode == 0

def build_benchmark_docker(dir):
    proc = Popen(["/bin/bash", "build-docker.sh"], cwd=dir)
    proc.wait()
    return proc.returncode == 0

def update_ray(ray_src_dir):
    git_fetch(dir=ray_src_dir)
    # TODO: which branch are we tracking? Use: git rev-parse --abbrev-ref --symbolic-full-name @{u}
    print "Ray HEAD is at", get_rev(refname="HEAD", dir=ray_src_dir)
    print "Ray origin/master is at", get_rev(refname="origin/master", dir=ray_src_dir)
    (ahead, behind) = get_relationship(refname="HEAD", other_refname="origin/master", dir=ray_src_dir)
    print "Ray HEAD is {} ahead {} behind origin/master".format(ahead, behind)
    if behind > 0:
        git_pull(dir=ray_src_dir)
        build_ray_docker(dir=ray_src_dir)
        return True
    else:
        return False

def update_benchmark(force_rebuild, benchmark_src_dir):
    git_fetch(dir=benchmark_src_dir)
    print "Benchmark HEAD is at", get_rev(refname="HEAD", dir=benchmark_src_dir)
    print "Benchmark origin/master is at", get_rev(refname="origin/master", dir=benchmark_src_dir)
    (ahead, behind) = get_relationship(refname="HEAD", other_refname="origin/master", dir=benchmark_src_dir)
    print "Benchmark HEAD is {} ahead {} behind origin/master".format(ahead, behind)
    if behind > 0:
        git_pull(dir=benchmark_src_dir)
        force_rebuild = True
    if force_rebuild
        build_benchmark_docker(dir=benchmark_src_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="matrix.py", description="Ray performance and stress testing matrix")
    parser.add_argument("configuration", metavar="config.json", help="json configuration file")
    parser.add_argument("--experiment-name", help="descriptive name for this experiment")
    parser.add_argument("--log-directory", default="logs", help="directory for log files")
    parser.add_argument("--continuous", action="store_true", help="run continuously")
    parser.add_argument("--ray-src", default="../ray", help="path to Ray sources, used when running continuously")
    parser.add_argument("--sleep", default=30, help="time to sleep between checks for new code, used when running continuously")
    args = parser.parse_args()

    with open(args.configuration) as f:
        config = json.load(f)

    if args.experiment_name:
        experiment_name = args.experiment_name
    else:
        experiment_name = os.path.splitext(os.path.basename(args.configuration))[0]

    if args.continuous:
        while True:
            ray_updated = update_ray(args.ray_src)
            update_benchmark(force_rebuild=ray_updated, benchmark_src_dir=".")
            do_sweep(experiment_name, config, args.log_directory)
    else:
        do_sweep(experiment_name, config, args.log_directory)

    print("Completed with output to {}".format(log_directory))
