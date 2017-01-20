import argparse
import base64
import json
import re
import os
from subprocess import Popen, PIPE

import event_stats

import raybench

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark Ray workloads across a range of cluster sizes")
    parser.add_argument("--workload", required=True, help="benchmark workload script")
    parser.add_argument("--output", required=True, help="output directory")
    parser.add_argument("--sweep-workers-arithmetic", help="sweep arithmetic: start:end:step")
    parser.add_argument("--workers-per-node", type=int, default=4, help="number of workers per node")
    parser.add_argument("--hosts", required=True, help="file containing host ip addresses")
    # parser.add_argument("--local", help="run locally")
    args = parser.parse_args()

    # create the output directory
    output_dir = args.output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if args.workload.endswith(".py"):
        workload_name = args.workload[:-3]
    else:
        workload_name = args.workload

    if args.sweep_workers_arithmetic:
        m = re.search("^([0-9]+):([0-9]+):([0-9]+)$", args.sweep_workers_arithmetic)
        sweep_start = int(m.group(1))
        sweep_end = int(m.group(2))
        sweep_step = int(m.group(3))
    else:
        raise RuntimeError("sweep not found")

    (master_ip, other_ips) = raybench.get_ips(args.hosts)
    print master_ip, other_ips
    cc = raybench.ClusterControl(master_ip, other_ips)
    num_workers_per_node = args.workers_per_node
    for num_nodes in range(sweep_start, sweep_end + 1, sweep_step):
        config_info = {
            "num_nodes" : num_nodes,
            "num_workers" : num_nodes * num_workers_per_node,
            "workload" : workload_name }
        cc.initialize(num_nodes, num_workers_per_node)
        cc.run_benchmark(args.workload, config_info)
        cc.download_stats("{}/{}_{}_{}.json.gz".format(output_dir, workload_name, num_nodes, num_workers_per_node))
