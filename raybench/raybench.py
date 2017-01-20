import argparse
import base64
import json
import re
import os
from subprocess import Popen, PIPE

import event_stats

class ClusterControl(object):
    def __init__(self, master_node, worker_nodes):
        self.master_node = master_node
        self.worker_nodes = worker_nodes
        self.all_nodes = [master_node] + worker_nodes
        self.username = 'ubuntu'

        self._reset_config()

    def _reset_config(self):
        self.num_nodes_started = None
        self.num_workers_started = None
        self.redis_address = None

    def initialize(self, num_nodes, num_workers_per_node):
        self._stop_ray(self.all_nodes)
        redis_address = self._start_ray_master_node(self.master_node, num_workers_per_node)
        if num_nodes > 1:
            self._start_ray_worker_node(self.worker_nodes[:(num_nodes-1)], num_workers_per_node, redis_address)
        self.num_nodes_started = num_nodes
        self.num_workers_started = num_nodes * num_workers_per_node
        self.redis_address = redis_address
        return redis_address

    def run_benchmark(self, benchmark_script, config):
        # TODO put benchmark script in the right place on the destination host
        self._scp_upload(self.master_node, ["lear.txt", benchmark_script])

        benchmark_command = "export PATH=/home/ubuntu/anaconda2/bin/:$PATH && source activate raydev && export RAY_REDIS_ADDRESS={} && export RAY_NUM_WORKERS={} && python {}".format(self.redis_address, self.num_workers_started, benchmark_script)
        self._pssh_command(self.master_node, benchmark_command)

        config_str = base64.b64encode(json.dumps(config))
        benchmark_stats_command = "export PATH=/home/ubuntu/anaconda2/bin/:$PATH && source activate raydev && export RAY_REDIS_ADDRESS={} && python -m raybench.stats --config=\"{}\"".format(self.redis_address, config_str)
        self._pssh_command(self.master_node, benchmark_stats_command)

    def download_stats(self, filename):
        self._scp_download(self.master_node, "benchmark_log.json.gz", filename)

    def _scp_upload(self, host, src, dst=""):
        if not isinstance(src, list):
            src = [src]
        args = ["scp"] + src + ["{}@{}:{}".format(self.username, host, dst)]
        proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = proc.communicate()
        print "STDOUT"
        print stdout
        print "STDERR"
        print stderr

    def _scp_download(self, host, src, dst):
        args = ["scp", "{}@{}:{}".format(self.username, host, src), dst]
        proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = proc.communicate()
        print "STDOUT"
        print stdout
        print "STDERR"
        print stderr

    def _pssh_command(self, hosts, command):
        if isinstance(hosts, list):
            host_args = []
            for host in hosts:
                host_args += ['--host', host]
        else:
                host_args = ['--host', hosts]
        args = ['pssh', '-l', self.username] + host_args + ['-t', '60', '-I', '-P']
        print hosts, ":", command
        proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = proc.communicate(input=command)
        print "STDOUT"
        print stdout
        print "STDERR"
        print stderr
        return stdout, stderr

    def _stop_ray(self, hosts):
        print "stop ray on host", hosts
        self._reset_config()
        self._pssh_command(hosts, 'ray/scripts/stop_ray.sh')

    def _start_ray_master_node(self, host, num_workers):
        print "starting master on {}".format(host)
        script = 'export PATH=/home/ubuntu/anaconda2/bin/:$PATH && source activate raydev && rm -f dump.rdb ray-benchmarks/dump.rdb benchmark_log.json.gz && ray/scripts/start_ray.sh --head --num-workers {}'.format(num_workers)
        stdout, stderr = self._pssh_command(host, script)
        m = re.search("([0-9\.]+:[0-9]+)'", stdout)
        redis_address = m.group(1)
        self.redis_address = redis_address
        return redis_address

    def _start_ray_worker_node(self, hosts, num_workers, redis_address):
        print "starting workers on {}".format(hosts)
        script = 'export PATH=/home/ubuntu/anaconda2/bin/:$PATH && source activate raydev && ray/scripts/start_ray.sh --num-workers {} --redis-address {}'.format(num_workers, redis_address)
        stdout, stderr = self._pssh_command(hosts, script)


def get_all_ips(hosts_file):
    with open(hosts_file) as f:
        return list([line.strip() for line in f.readlines()])

def get_ips(hosts_file, num_nodes=None):
    ips = get_all_ips(hosts_file)
    master_ip = ips[0]
    if num_nodes is not None:
        if len(all_ips) < num_nodes:
            raise RuntimeError("not enough nodes available")
        other_ips = ips[1:num_nodes]
    else:
        other_ips = ips[1:]
    return master_ip, other_ips
