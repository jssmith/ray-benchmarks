from __future__ import print_function

import argparse
import datetime
import raybench.eventstats as eventstats
import json
import sys

from itertools import groupby
from os import listdir
from os.path import join, isdir, basename, dirname, realpath

from tabulate import tabulate


class Analysis(object):
    def __init__(self):
        self.all_stats = []

    class FileProcessor(object):
        def __init__(self):
            self.handlers = {
                "experiment" : self.handle_experiment_event,
                "finish_work" : self.handle_finish_work_event
            }
            self.timestamp = None
            self.experiment_config = None
            self.analysis = eventstats.Analysis()
            self.fail_ct = 0
            self.max_success_iteration = 0

        def handle_event(self, e):
            event_type = e["type"]
            if event_type in self.handlers:
                self.handlers[event_type](e)

        def handle_experiment_event(self, e):
            if self.experiment_config:
                raise RuntimeError("Duplicate experiment config")
            else:
                self.experiment_config = e["data"]
                self.timestamp = e["timestamp"]

        def handle_finish_work_event(self, e):
            if e["data"]["success"]:
                [self.analysis.add_event(ev) for ev in e["data"]["stats"]["events"]]
                if e["data"]["iteration"] > self.max_success_iteration:
                    self.max_success_iteration = e["data"]["iteration"]
            else:
                self.fail_ct += 1

        def get_analysis(self):
            if not self.experiment_config:
                raise RuntimeError("No experiment config")
            a = {}
            a["summary_stats"] = self.analysis.summary_stats()
            a["fail_ct"] = self.fail_ct
            a["max_success_iteration"] = self.max_success_iteration
            a["experiment_config"] = self.experiment_config
            a["timestamp"] = self.timestamp
            return a

    def add_file(self, filename):
        with open(filename, "r") as f:
            try:
                fp = Analysis.FileProcessor()
                for line in f:
                    fp.handle_event(json.loads(line))
                self.all_stats.append(fp.get_analysis())
            except(RuntimeError):
                print("skipping file {}".format(filename), file=sys.stderr)

    def summary_table(self):
        stat_names = { "min_elapsed_time" : "min",
            "max_elapsed_time" : "max",
            "avg_elapsed_time" : "avg",
            "ct" : "ct"
            }
        for s in self.all_stats:
            system_config = s["experiment_config"]["system_config"]
            num_workers = system_config["num_workers"]
            num_nodes = system_config["num_nodes"]
            name = s["experiment_config"]["workload"]["name"]
            if "git-revs" in s["experiment_config"]:
                ray_git_rev = s["experiment_config"]["git-revs"]["ray"][:7]
                benchmark_git_rev = s["experiment_config"]["git-revs"]["benchmark"][:7]
            else:
                ray_git_rev = None
                benchmark_git_rev = None
            keyprefixlen = len("None:None:")
            for key, stats in s["summary_stats"].items():
                measurement = key[keyprefixlen:]
                for stat in [ "min_elapsed_time", "max_elapsed_time", "avg_elapsed_time", "ct" ]:
                    print(ray_git_rev, benchmark_git_rev, name, num_workers, num_nodes, measurement, stat_names[stat], stats[stat])
            print(ray_git_rev, benchmark_git_rev, name, num_workers, num_nodes, "success_iteration", "max", s["max_success_iteration"])
            print(ray_git_rev, benchmark_git_rev, name, num_workers, num_nodes, "failures", "ct", s["fail_ct"])

    def workload_trend(self):

        def stat_basic_info(s):
            system_config = s["experiment_config"]["system_config"]
            num_workers = system_config["num_workers"]
            num_nodes = system_config["num_nodes"]
            name = s["experiment_config"]["workload"]["name"]
            return name, num_workers, num_nodes

        def dateformat(timestamp):
            return datetime.datetime.fromtimestamp(timestamp).strftime('%Y%m%d %H:%M:%S')

        with open(join(dirname(dirname(realpath(__file__))),"config/analyze.json")) as f:
            analysis_json = json.load(f)
            analysis_info = {}
            for w in analysis_json["workloads"]:
                analysis_info[w["name"]] = w

        stat_groups = {k : list(v) for k, v in groupby(sorted(self.all_stats, key=stat_basic_info), key=stat_basic_info)}

        for k in sorted(stat_groups.keys()):
            name, num_workers, num_nodes = k
            filter_col = lambda x: x
            if name in analysis_info:
                description = analysis_info[name]["description"]
                if "cols" in analysis_info[name]:
                    cols = analysis_info[name]["cols"]
                    filter_col = lambda x,cols=cols: cols[x] if x in cols else None
            else:
                description = "Unknown description"

            g = stat_groups[k]
            print("\n==================================================================================================================\n")
            print("workload:", name)
            print("num workers:", num_workers)
            print("num nodes:", num_nodes)
            print(description)
            print()

            names = []
            rows = []

            def add_col(name, stat):
                include_col = filter_col(name)
                if include_col:
                    row.append(stat)
                    row_names.append(include_col)

            std_names = ["timestamp", "ray rev", "bm rev", "workload", "workers", "nodes"]
            for s in g:
                timestamp = s["timestamp"]
                if "git-revs" in s["experiment_config"]:
                    ray_git_rev = s["experiment_config"]["git-revs"]["ray"][:7]
                    benchmark_git_rev = s["experiment_config"]["git-revs"]["benchmark"][:7]
                else:
                    ray_git_rev = None
                    benchmark_git_rev = None
                keyprefixlen = len("None:None:")
                row = [dateformat(timestamp), ray_git_rev, benchmark_git_rev, name, num_workers, num_nodes]
                row_names = []
                for key, stats in s["summary_stats"].items():
                    measurement = key[keyprefixlen:]
                    for stat in [ "min_elapsed_time", "max_elapsed_time", "avg_elapsed_time", "ct" ]:
                        stat_name = "{}_{}".format(measurement, stat)
                        add_col(stat_name, stats[stat])

                if row_names:
                    add_col("success_iteration_max", s["max_success_iteration"])
                    add_col("failures_ct", s["fail_ct"])

                    if not names:
                        names = row_names
                    elif row_names != names:
                        print("{} is not {}".format(row_names, names))
                        raise RuntimeError("inconsistent column names")
                    rows.append(row)

            list.sort(rows)
            print(tabulate(rows, headers=(std_names + names), floatfmt=".3f"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="analyze.py", description="Analyze logs of Ray performance and stress testing")
    parser.add_argument("logdirectory", help="json configuration file")
    parser.add_argument("--no-recurse", action="store_true", help="look for log files recursively")
    parser.add_argument("--human-readable", action="store_true", help="output for human reader")
    parser.add_argument("--filter", help="filter on log file names (select workload)")
    args = parser.parse_args()

    if args.filter:
        filter = lambda fn: basename(fn).startswith(args.filter)
    else:
        filter=lambda _:True

    def findfiles(dir):
        all_files = []
        listing = listdir(dir)
        list.sort(listing)
        for name in listing:
            name_path = join(dir, name)
            if isdir(name_path):
                if not args.no_recurse:
                    all_files += findfiles(name_path)
            else:
                if filter(name_path):
                    all_files.append(name_path)
        return all_files

    a = Analysis()
    for f in findfiles(args.logdirectory):
        a.add_file(f)
    if args.human_readable:
        a.workload_trend()
    else:
        a.summary_table()
