import re
import sys
import time
import logging
import json

from subprocess import Popen, PIPE

class Logger(object):
    def __init__(self, fn):
        self.f = open(fn, "a")

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
        self.container_id = None
        self.container_ip = None

    def _get_container_id(self, stdoutdata):
        p = re.compile("([0-9a-f]{64})\n")
        m = p.match(stdoutdata)
        if not m:
            return None
        else:
            return m.group(1)

    def _get_container_ip(self, container_id):
        proc = Popen(["docker", "inspect", "--format='{{.NetworkSettings.Networks.bridge.IPAddress}}'", container_id], stdout=PIPE)
        (stdoutdata, _) = proc.communicate()
        p = re.compile("([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})")
        m = p.match(stdoutdata)
        if not m:
            raise RuntimeError("Container IP not found")
        else:
            return m.group(1)

    def start_ray(self, shm_size, num_workers):
        proc = Popen(["docker", "run", "-d", "--shm-size=" + shm_size, "ray-project/benchmark", "/ray/scripts/start_ray.sh", "--head", "--redis-port=6379", "--num-workers=4"], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        container_id = self._get_container_id(stdoutdata)
        self.logger.log("ray_start", {
            "container_id" : container_id,
            "shm_size" : shm_size
            })
        if not container_id:
            raise RuntimeError("Failed to find container id")
        self.container_id = container_id
        self.num_workers = num_workers
        self.container_ip = self._get_container_ip(container_id)
        return container_id

    def run_benchmark(self, workload_script):
        proc = Popen(["docker", "exec",
            self.container_id,
            "/bin/bash", "-c",
            "RAY_BENCHMARK_ENVIRONMENT=stress RAY_REDIS_ADDRESS={}:6379 RAY_NUM_WORKERS={} python {}".format(self.container_ip, self.num_workers, workload_script)], stdout=PIPE, stderr=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        print stdoutdata
        print stderrdata
        print "return code", proc.returncode
        if proc.returncode == 0:
            return True
        else:
            return False

    def stop_ray(self):
        proc = Popen(["docker", "kill", self.container_id], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        stopped_container_id = self._get_container_id(stdoutdata)
        if not stopped_container_id or self.container_id != stopped_container_id:
            raise RuntimeError("Failed to stop container {}".format(self.container_id))

    def _do_iteration(self, iteration_state):
        i = iteration_state.iteration_index
        iteration_state.iteration_index += 1
        self.logger.log("start_work", {
            "iteration" : i
            })
        start_time = time.time()
        success = self.run_benchmark(workload_script)
        elapsed_time = time.time() - start_time
        self.logger.log("finish_work", {
                "iteration" : i,
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
        if iteration_target and not time_target:
            loop_predicate = loop_predicate_iteration
        elif time_target and not iteration_target:
            loop_predicate = loop_predicate_time
        else:
            loop_predicate = lambda: loop_predicate_time() and loop_predicate_iteration()

        while loop_predicate():
            if not self._do_iteration(iteration_state):
                break
        iteration_state.iteration_end_time = time.time()

        logger.log("finish_iteration", {
            "workload_script" : workload_script,
            "excessive_failures" : iteration_state.excessive_failures,
            "num_successes" : iteration_state.num_successes,
            "num_failures" : iteration_state.num_failures,
            "elapsed_time" : iteration_state.iteration_end_time - iteration_state.iteration_start_time,
            "iteration_end_reason" : iteration_state.iteration_end_reason
            })

if __name__ == "__main__":
    workload_script = sys.argv[1]
    logger = Logger("log.txt")
    s = StressRay(logger)
    container_id = s.start_ray(shm_size="1G", num_workers=4)

    # sleep a little bit to give Ray time to start
    time.sleep(2)

    # s.iterate_workload(workload_script, iteration_target=10)
    s.iterate_workload(workload_script, time_target=20)

    s.stop_ray()
