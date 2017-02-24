import re
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
        # o = {
        #     "type" : event_type,
        #     "timestamp" : time.time(),
        #     "data" : event_data
        #     }
        # print(o)
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
        self.container_id = None

    def _get_container_id(self, stdoutdata):
        p = re.compile("([0-9a-f]{64})\n")
        m = p.match(stdoutdata)
        if not m:
            return None
        else:
            return m.group(1)

    def start_ray(self):
        shm_size = "1G"
        proc = Popen(["docker", "run", "-d", "--shm-size=" + shm_size, "ray-project/benchmark", "/ray/scripts/start_ray.sh", "--head", "--redis-port=6379", "--num-workers=4"], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        container_id = self._get_container_id(stdoutdata)
        self.logger.log("ray_start", {
            "container_id" : container_id,
            "shm_size" : shm_size
            })
        if not container_id:
            raise RuntimeError("Failed to find container id")
        else:
            self.container_id = container_id
            return container_id

    def run_benchmark(self):
        proc = Popen(["docker", "exec", self.container_id, "python", "/benchmark/workloads/matmul_ray.py"], stdout=PIPE)
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

if __name__ == "__main__":
    logger = Logger("log.txt")
    s = StressRay(logger)
    container_id = s.start_ray()
    print "started container", container_id
    time.sleep(2)
    sequential_failures = 0
    for i in range(10000):
        logger.log("start_work", {
            "iteration" : i
            })
        start_time = time.time()
        success = s.run_benchmark()
        elapsed_time = time.time() - start_time
        logger.log("finish_work", {
                "iteration" : i,
                "elapsed_time" : elapsed_time,
                "success" : success
            })

        if success:
            sequential_failures = 0
        else:
            sequential_failures += 1
            if sequential_failures >= 3:
                print "excessive failures"
                break
    s.stop_ray()
