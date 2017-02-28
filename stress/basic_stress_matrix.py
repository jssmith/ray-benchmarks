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
        s.start_ray(shm_size=system_config["shm_size"], mem_size=system_config["mem_size"], num_workers=system_config["num_workers"], num_nodes=system_config["num_nodes"])
        time.sleep(2)
        execution_time_limit = workload["execution_time_limit"] if "execution_time_limit" in workload else None
        s.iterate_workload(workload["workload_script"], iteration_target=workload["iteration_target"], time_target=workload["time_target"], execution_time_limit=execution_time_limit)
        s.stop_ray()

    # analysis


if __name__ == "__main__":
    experiment_name = "run2"
    system_configs = [ 
        { "num_workers" : 100, "num_nodes" : 1, "shm_size" : "1G", "mem_size" : None },
        { "num_workers" : 100, "num_nodes" : 2, "shm_size" : "1G", "mem_size" : None }
        ]

    workloads = [
        {
            "name" : "launch_donothing",
            "workload_script" : "workloads/donothing10k.py",
            "iteration_target" : 10,
            "time_target" : None,
            "execution_time_limit" : None
        },
        {
            "name" : "sleep_donothing",
            "workload_script" : "workloads/sleep1s10k.py",
            "iteration_target" : 10,
            "time_target" : None,
            "execution_time_limit" : None
        },
        {
            "name" : "worker_blocking",
            "workload_script" : "workloads/blocking.py",
            "iteration_target" : None,
            "time_target" : 3600,
            "execution_time_limit" : 10
        }
        ]

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    for system_config in system_configs:
        for workload in workloads:
            run_experiment(experiment_name, workload, system_config)