import os
import time

from stressloop import Logger, StressRay

log_directory = "./logs"

def run_experiment(experiment_name, workload, system_config):
    log_filename = "{}/{}_{}_{}_{}_{}.log".format(
        log_directory,
        experiment_name,
        workload["name"],
        system_config["num_nodes"],
        system_config["num_workers"],
        system_config["shm_size"])
    with Logger(log_filename) as logger:
        logger.log("experiment", {
            "experiment_name" : experiment_name,
            "system_config" : system_config,
            "workload" : workload
            })
        s = StressRay(logger)
        s.start_ray(shm_size=system_config["shm_size"], num_workers=system_config["num_workers"], num_nodes=system_config["num_nodes"])
        time.sleep(2)
        s.iterate_workload(workload["workload_script"], iteration_target=10, time_target=None)
        s.stop_ray()

    # analysis


if __name__ == "__main__":
    experiment_name = "run1"
    system_configs = [ 
        { "num_workers" : 10, "num_nodes" : 1, "shm_size" : "1G" },
        { "num_workers" : 10, "num_nodes" : 2, "shm_size" : "1G"}
        ]

    workloads = [
        { "name" : "launch_donothing", "workload_script" : "workloads/donothing10k.py" },
        { "name" : "sleep_donothing", "workload_script" : "workloads/sleep1s10k.py" }
        ]

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    for system_config in system_configs:
        for workload in workloads:
            run_experiment(experiment_name, workload, system_config)