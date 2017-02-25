import argparse
import json
import re
import sys
import time

from subprocess import Popen, PIPE

class Logger(object):
    def __init__(self, fn):
        if fn:
            self.f = open(fn, "a")
        else:
            self.f = sys.stdout

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.f.close()

    def log(self, event_type, event_data):
        json.dump({
            "type" : event_type,
            "timestamp" : time.time(),
            "data" : event_data
            }, self.f)
        self.f.write("\n")
        self.f.flush()

    def log_txt(self, msg_txt):
        self.f.write(msg_txt + "\n")
        self.f.flush()


class StressRay(object):

    def __init__(self, logger):
        self.logger = logger
        self.num_workers = None
        self.head_container_id = None
        self.worker_container_ids = []
        self.head_container_ip = None

    def _get_container_id(self, stdoutdata):
        p = re.compile("([0-9a-f]{64})\n")
        m = p.match(stdoutdata)
        if not m:
            return None
        else:
            return m.group(1)

    def _get_container_ip(self, container_id):
        proc = Popen(["docker", "inspect", "--format={{.NetworkSettings.Networks.bridge.IPAddress}}", container_id], stdout=PIPE, stderr=PIPE)
        (stdoutdata, _) = proc.communicate()
        p = re.compile("([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})")
        m = p.match(stdoutdata)
        if not m:
            raise RuntimeError("Container IP not found")
        else:
            return m.group(1)

    def _start_head_node(self, shm_size, num_workers):
        proc = Popen(["docker", "run", "-d", "--shm-size=" + shm_size, "ray-project/benchmark", "/ray/scripts/start_ray.sh", "--head", "--redis-port=6379", "--num-workers={:d}".format(num_workers)], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        container_id = self._get_container_id(stdoutdata)
        self.logger.log("start_node", {
            "container_id" : container_id,
            "is_head" : True,
            "num_workers" : num_workers,
            "shm_size" : shm_size
            })
        if not container_id:
            raise RuntimeError("Failed to find container id")
        self.head_container_id = container_id
        self.head_container_ip = self._get_container_ip(container_id)
        return container_id

    def _start_worker_node(self, shm_size, num_workers):
        proc = Popen(["docker", "run", "-d", "--shm-size=" + shm_size, "ray-project/benchmark", "/ray/scripts/start_ray.sh", "--redis-address={:s}:6379".format(self.head_container_ip), "--num-workers={:d}".format(num_workers)], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        container_id = self._get_container_id(stdoutdata)
        if not container_id:
            raise RuntimeError("Failed to find container id")
        self.worker_container_ids.append(container_id)
        self.logger.log("start_node", {
            "container_id" : container_id,
            "is_head" : False,
            "num_workers" : num_workers,
            "shm_size" : shm_size
            })

    def start_ray(self, shm_size, num_workers, num_nodes):
        if num_workers < num_nodes:
            raise RuntimeError("number of workers must exceed number of nodes")
        self.num_workers = num_workers
        total_procs = num_workers + 2
        workers_per_node_a = int(total_procs / num_nodes)
        workers_per_node_b = workers_per_node_a + 1
        n_a = workers_per_node_b * num_nodes - total_procs
        n_b = num_nodes - n_a

        if n_b > 0:
            workers_per_node_h = workers_per_node_b - 2
            n_b = n_b - 1
        else:
            workers_per_node_h = workers_per_node_a - 2
            n_a = n_a - 1

        # launch the head node
        self._start_head_node(shm_size, workers_per_node_h)
        for _ in range(n_a):
            self._start_worker_node(shm_size, workers_per_node_a)
        for _ in range(n_b):
            self._start_worker_node(shm_size, workers_per_node_b)

    def run_benchmark(self, workload_script):
        proc = Popen(["docker", "exec",
            self.head_container_id,
            "/bin/bash", "-c",
            "RAY_BENCHMARK_ENVIRONMENT=stress RAY_REDIS_ADDRESS={}:6379 RAY_NUM_WORKERS={} python {}".format(self.head_container_ip, self.num_workers, workload_script)], stdout=PIPE, stderr=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        print stdoutdata
        print stderrdata
        print "return code", proc.returncode
        if proc.returncode == 0:
            return True
        else:
            return False

    def _stop_node(self, container_id):
        proc = Popen(["docker", "kill", container_id], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        stopped_container_id = self._get_container_id(stdoutdata)
        stop_successful = container_id == stopped_container_id
        self.logger.log("stop_node", {
            "container_id" : container_id,
            "is_head" : container_id == self.head_container_id,
            "success" : stop_successful
            })

    def stop_ray(self):
        self._stop_node(self.head_container_id)
        for container_id in self.worker_container_ids:
            self._stop_node(container_id)

    def _do_iteration(self, workload_script, iteration_state):
        i = iteration_state.iteration_index
        iteration_state.iteration_index += 1
        self.logger.log("start_work", {
            "head_container_id" : self.head_container_id,
            "iteration" : i
            })
        start_time = time.time()
        success = self.run_benchmark(workload_script)
        elapsed_time = time.time() - start_time
        self.logger.log("finish_work", {
                "iteration" : i,
                "head_container_id" : self.head_container_id,
                "workload_script" : workload_script,
                "elapsed_time" : elapsed_time,
                "success" : success
            })

        if success:
            iteration_state.num_successes += 1
            iteration_state.sequential_failures = 0
            continue_iteration = True
        else:
            iteration_state.num_failures += 1
            iteration_state.sequential_failures += 1
            if iteration_state.sequential_failures >= 3:
                iteration_state.iteration_end_reason = "excessive_failures"
                continue_iteration = False
            else:
                continue_iteration = True
        return continue_iteration

    def iterate_workload(self, workload_script, iteration_target=None, time_target=None):
        class iteration_state: pass
        iteration_state.sequential_failures = 0
        iteration_state.excessive_failures = False
        iteration_state.iteration_index = 0
        iteration_state.num_successes = 0
        iteration_state.num_failures = 0
        iteration_state.iteration_start_time = time.time()
        iteration_state.iteration_end_reason = "target_reached"

        loop_predicate_iteration = lambda: iteration_state.iteration_index < iteration_target
        loop_predicate_time = lambda: time.time() - iteration_state.iteration_start_time < time_target
        if not iteration_target and not time_target:
            raise RuntimeError("Must provide iteration target and / or time target")
        elif iteration_target and not time_target:
            loop_predicate = loop_predicate_iteration
        elif time_target and not iteration_target:
            loop_predicate = loop_predicate_time
        else:
            loop_predicate = lambda: loop_predicate_time() and loop_predicate_iteration()

        logger.log("start_iterations", {
            "workload_script" : workload_script,
            "head_container_id" : self.head_container_id,
            "iteration_target" : iteration_target,
            "time_target" : time_target
            })
        while loop_predicate():
            if not self._do_iteration(workload_script, iteration_state):
                break
        iteration_state.iteration_end_time = time.time()

        logger.log("finish_iterations", {
            "workload_script" : workload_script,
            "num_successes" : iteration_state.num_successes,
            "num_failures" : iteration_state.num_failures,
            "elapsed_time" : iteration_state.iteration_end_time - iteration_state.iteration_start_time,
            "iteration_end_reason" : iteration_state.iteration_end_reason
            })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="stressloop.py", description="Plot Ray workloads")
    parser.add_argument("--workload", required=True, help="workload script")
    parser.add_argument("--shm-size", default="1G", help="shared memory size")
    parser.add_argument("--num-workers", default=4, type= int, help="number of workers")
    parser.add_argument("--num-nodes", default=1, type=int, help="number of instances")
    parser.add_argument("--time-target", type=int, help="time target in seconds")
    parser.add_argument("--iteration-target", type=int, help="iteration target in seconds")
    parser.add_argument("--log", help="event log file")
    args = parser.parse_args()

    if not args.time_target and not args.iteration_target:
        parser.error("must provide --time-target, --iteration-target, or both")

    with Logger(args.log) as logger:
        s = StressRay(logger)
        s.start_ray(shm_size=args.shm_size, num_workers=args.num_workers, num_nodes=args.num_nodes)

        # sleep a little bit to give Ray time to start
        time.sleep(2)

        s.iterate_workload(args.workload, iteration_target=args.iteration_target, time_target=args.time_target)

        s.stop_ray()
