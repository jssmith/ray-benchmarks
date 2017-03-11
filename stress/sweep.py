import argparse
import boto3
import json
import os
import sys
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
        ray_git_rev, benchmark_git_rev = get_docker_git_revs("ray-project/benchmark", ["/ray/git-rev", "/benchmark/git-rev"])
        logger.log("experiment", {
            "experiment_name" : experiment_name,
            "system_config" : system_config,
            "workload" : workload,
            "git-revs" : {
                "ray" : ray_git_rev,
                "benchmark" : benchmark_git_rev
                }
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
    print("get rev {} {}".format(refname, dir))
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

def get_docker_git_revs(docker_image, paths):
    proc = Popen(["docker", "run", docker_image, "cat"] + paths, stdout=PIPE)
    (stdout, _) = proc.communicate()
    git_revs = stdout.strip().split('\n')
    if len(git_revs) != len(paths):
        return len(paths) * [""]
    return git_revs

def git_fetch(dir="."):
    Popen(["git", "fetch"], cwd=dir).wait()

def git_pull(dir="."):
    Popen(["git", "pull"], cwd=dir).wait()

def git_get_tracked(dir="."):
    proc = Popen(["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"], cwd=dir, stdout=PIPE)
    (stdout, _) = proc.communicate()
    return stdout.strip()

def build_ray_docker(dir):
    proc = Popen(["/bin/bash", "build-docker.sh", "--skip-examples"], cwd=dir)
    proc.wait()
    return proc.returncode == 0

def build_benchmark_docker(dir):

    proc.wait()
    return proc.returncode == 0

def update(src_dir, respawn_if_updated=False):
    initial_git_rev = get_rev(refname="HEAD", dir=src_dir)
    git_fetch(dir=src_dir)
    tracking_ref = git_get_tracked(dir=src_dir)
    if not tracking_ref:
        print "no upstream found"
        return initial_git_rev
    print "{} is tracking {}".format(src_dir, tracking_ref)
    print "HEAD is at", initial_git_rev
    print "{} is at {}".format(tracking_ref, get_rev(refname=tracking_ref, dir=src_dir))
    (ahead, behind) = get_relationship(refname="HEAD", other_refname=tracking_ref, dir=src_dir)
    print "Ray HEAD is {} ahead {} behind {}".format(ahead, behind, tracking_ref)
    if behind > 0:
        git_pull(dir=src_dir)
        if respawn_if_updated:
            executable = sys.executable
            os.execl(executable, executable, *sys.argv)
        else:
            return get_rev(refname="HEAD", dir=src_dir)
    else:
        return initial_git_rev

def build_benchmark(ray_git_rev, benchmark_git_rev, benchmark_src_dir):
    docker_git_revs = get_docker_git_revs("ray-project/benchmark", ["/ray/git-rev", "/benchmark/git-rev"])
    if docker_git_revs != [ray_git_rev, benchmark_git_rev]:
        if call(["/bin/bash", "build-docker.sh"], cwd=benchmark_src_dir) != 0:
            raise RuntimeError("error rebuilding benchmark Docker image")

def build_ray(ray_git_rev, ray_src_dir):
    docker_git_revs = get_docker_git_revs("ray-project/deploy", ["/ray/git-rev"])
    if docker_git_revs != [ray_git_rev]:
        print("building ray as {} does not match {}".format(docker_git_revs[0], ray_git_rev))
        if call(["/bin/bash", "build-docker.sh", "--skip-examples"], cwd=ray_src_dir) != 0:
            raise RuntimeError("error rebuilding Ray Docker image")

def script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def post_analysis_s3(log_directory, s3_bucket):
    try:
        analysis_script = os.path.join(script_dir(), "analyze.py")
        proc = Popen(["python", analysis_script, "--human-readable", "--unique-revs", log_directory], stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = proc.communicate()
        if proc.returncode==0:
            s3 = boto3.resource('s3')
            s3.Bucket(s3_bucket).put_object(Key="latest.txt", Body=stdout, ContentType='text/plain')
            print("uploaded analysis to S3")
        else:
            print("failed up update analysis")
            print(stdout)
            print(stderr)
    except Exception as e:
        print("problem posting to S3", str(e))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="matrix.py", description="Ray performance and stress testing matrix")
    parser.add_argument("configuration", metavar="config.json", help="json configuration file")
    parser.add_argument("--experiment-name", help="descriptive name for this experiment")
    parser.add_argument("--log-directory", default="logs", help="directory for log files")
    parser.add_argument("--continuous", action="store_true", help="run continuously")
    parser.add_argument("--rebuild", action="store_true", help="force rebuild of Docker images")
    parser.add_argument("--ray-src", default="../ray", help="path to Ray sources, used when running continuously")
    parser.add_argument("--post-analysis-s3", help="upload analysis to s3 bucket")
    args = parser.parse_args()

    with open(args.configuration) as f:
        config = json.load(f)

    if args.experiment_name:
        experiment_name = args.experiment_name
    else:
        experiment_name = os.path.splitext(os.path.basename(args.configuration))[0]

    if args.continuous:
        benchmark_src_dir=os.path.dirname(script_dir())
        while True:
            benchmark_git_rev = update(src_dir=benchmark_src_dir, respawn_if_updated=True)
            ray_git_rev = update(src_dir=args.ray_src)
            build_ray(ray_git_rev, args.ray_src)
            build_benchmark(ray_git_rev, benchmark_git_rev, benchmark_src_dir)
            do_sweep(experiment_name, config, args.log_directory)
            if args.post_analysis_s3:
                post_analysis_s3(args.log_directory, args.post_analysis_s3)
    else:
        do_sweep(experiment_name, config, args.log_directory)

    print("Completed with output to {}".format(log_directory))
