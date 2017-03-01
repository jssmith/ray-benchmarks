import argparse
import json
import os
import time

from loop import Logger, StressRay

def run_experiment(experiment_name, workload, system_config, log_directory):
    log_filename = os.path.join(log_directory, "{}_{}_{}_{}_{}.log".format(
        experiment_name,
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
        s.iterate_workload(workload["workload_script"], iteration_target=workload["iteration_target"], time_target=workload["time_target"], execution_time_limit=execution_time_limit)
        s.stop_ray()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="matrix.py", description="Ray performance and stress testing matrix")
    parser.add_argument("configuration", metavar="config.json", help="json configuration file")
    parser.add_argument("--experiment-name", help="descriptive name for this experiment")
    parser.add_argument("--log-directory", default="logs", help="directory for log files")
    args = parser.parse_args()

    with open(args.configuration) as f:
        config = json.load(f)

    log_directory = os.path.join(args.log_directory, time.strftime("%Y%m%d_%H%M%S"))
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    print("Output to {}".format(log_directory))

    if args.experiment_name:
        experiment_name = args.experiment_name
    else:
        experiment_name = os.path.splitext(os.path.basename(args.configuration))[0]

    for system_config in config["system_configs"]:
        for workload in config["workloads"]:
            run_experiment(experiment_name, workload, system_config, log_directory)

    print("Completed with output to {}".format(log_directory))