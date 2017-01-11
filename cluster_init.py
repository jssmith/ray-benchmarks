import sys
import re

from subprocess import call, Popen, PIPE


def get_all_ips():
    with open("host_ips.txt") as f:
        return list([line.strip() for line in f.readlines()])

def get_ips(num_nodes):
    all_ips = get_all_ips()
    if len(all_ips) < num_nodes:
        raise RuntimeError("not enough nodes available")
    master_ip = all_ips[0]
    other_ips = all_ips[1:(num_nodes - 1)]
    return master_ip, other_ips

def pssh_command(hosts, command):
    if isinstance(hosts, list):
        host_args = []
        for host in hosts:
            host_args += ['--host', host]
    else:
            host_args = ['--host', hosts]
    args = ['pssh', '-l', 'ubuntu'] + host_args + ['-t', '60', '-I', '-P']
    print hosts, ":", command
    proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = proc.communicate(input=command)
    print "STDOUT"
    print stdout
    print "STDERR"
    print stderr
    return stdout, stderr

def stop_ray(hosts):
    print "stop ray on host", hosts
    pssh_command(hosts, 'ray/scripts/stop_ray.sh')

def start_ray_master_node(host, num_workers):
    print "starting master on {}".format(host)
    script = 'export PATH=/home/ubuntu/anaconda2/bin/:$PATH && source activate raydev && rm -f dump.rdb ray-benchmarks/dump.rdb && ray/scripts/start_ray.sh --head --num-workers {}'.format(num_workers)
    stdout, stderr = pssh_command(host, script)
    m = re.search("([0-9\.]+:[0-9]+)'", stdout)
    redis_address = m.group(1)
    return redis_address

def start_ray_worker_node(host, num_workers, redis_address):
    print "starting worker on {}".format(host)
    script = 'export PATH=/home/ubuntu/anaconda2/bin/:$PATH && source activate raydev && ray/scripts/start_ray.sh --num-workers {} --redis-address {}'.format(num_workers, redis_address)
    stdout, stderr = pssh_command(host, script)

def init_cluster(num_nodes, num_workers_per_node):
    (master_ip, other_ips) = get_ips(num_nodes)

    redis_address = start_ray_master_node(master_ip, num_workers_per_node)
    for host in other_ips:
        start_ray_worker_node(host, num_workers_per_node, redis_address)
    return redis_address

def shutdown_cluster():
    stop_ray(get_all_ips())

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: cluster_init.py num_nodes num_workers_per_node"
        sys.exit(1)
    num_nodes = int(sys.argv[1])
    num_workers_per_node = int(sys.argv[2])
    shutdown_cluster()
    redis_address = init_cluster(num_nodes, num_workers_per_node)
    print "redis address is", redis_address
