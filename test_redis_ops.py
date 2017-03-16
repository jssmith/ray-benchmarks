from __future__ import print_function

import ray
import redis
import time

class RedisStats(object):
    class RedisSpan(object):
        def __init__(self, rs, name, scale):
            self._name = name
            self._rs = rs
            self._scale = scale

        def _diffs(self, before_info, after_info, interested_keys):
            diffs = {}
            for key in interested_keys:
                diffs[key] = after_info[key] - before_info[key]
            return diffs

        def __enter__(self):
            print("entering {}".format(self._name))
            self._before_info = self._rs._redis.info()

        def __exit__(self, exc_type, exc_val, exc_tb):
            print("exiting {}".format(self._name))
            after_info = self._rs._redis.info()
            self._rs._stats.append({
                    "name" : self._name,
                    "scale" : self._scale,
                    "stats" : self._diffs(self._before_info, after_info, self._rs._interested_keys)
                })

    def __init__(self, redis_address):
        host, port = redis_address.split(':')
        port = int(port)
        self._redis = redis.StrictRedis(host, port)
        self._stats = []
        self._interested_keys = interested_keys = ["total_commands_processed", "total_net_input_bytes", "total_net_output_bytes"]

    def span(self, name, scale):
        return RedisStats.RedisSpan(self, name, scale)

    def print_stats(self):
        for stat in self._stats:
            name = stat["name"]
            scale = stat["scale"]
            scaled_stats = [float(stat["stats"][key]) / float(scale) for key in self._interested_keys]
            print(name, scale, *scaled_stats)

@ray.remote
def f():
    return None

if __name__ == "__main__":
    address_info = ray.init()
    redis_address = address_info["redis_address"]
    s = RedisStats(redis_address)
    for scale in [100, 1000, 10000]:
        with s.span("invoke only", scale):
            [f.remote() for _ in range(scale)]            
            time.sleep(2)

        with s.span("invoke wait", scale):
            ray.wait([f.remote() for _ in range(scale)], num_returns=scale)
            time.sleep(2)

        with s.span("invoke get", scale):
            ray.get([f.remote() for _ in range(scale)])
            time.sleep(2)

    s.print_stats()
