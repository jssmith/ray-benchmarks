from __future__ import print_function

import argparse
import datetime
import raybench.eventstats as eventstats
import json
import sys

from collections import defaultdict
from itertools import groupby
from os import listdir
from os.path import join, isdir, basename, dirname, realpath

from tabulate import tabulate


class Analysis(object):
    def __init__(self):
        self.all_stats = []
        self.seen_git_revs = defaultdict(lambda: set())
        self.analysis_config = None

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
            self.max_success_iteration = -1
            self.max_success_workload_config = None
            self.after_experiment = lambda: None

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
                self.after_experiment()

        def handle_finish_work_event(self, e):
            if e["data"]["success"]:
                [self.analysis.add_event(ev) for ev in e["data"]["stats"]["events"]]
                if e["data"]["iteration"] > self.max_success_iteration:
                    self.max_success_iteration = e["data"]["iteration"]
                    self.max_success_workload_config = e["data"]["stats"]["config"]
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
            a["max_success_workload_config"] = self.max_success_workload_config
            a["timestamp"] = self.timestamp
            return a

    def update_revs(self, experiment_config):
        if "git-revs" in experiment_config:
            ray_git_rev = experiment_config["git-revs"]["ray"]
            benchmark_git_rev = experiment_config["git-revs"]["benchmark"]
        else:
            ray_git_rev = None
            benchmark_git_rev = None
        combined_rev = (ray_git_rev, benchmark_git_rev)
        seen_revs = self.seen_git_revs[self.stat_basic_info(experiment_config)]
        if combined_rev in seen_revs:
            return False
        else:
            seen_revs.add(combined_rev)
            return True

    def stat_basic_info(self, experiment_config):
        system_config = experiment_config["system_config"]
        num_workers = system_config["num_workers"]
        num_nodes = system_config["num_nodes"]
        name = experiment_config["workload"]["name"]
        return name, num_workers, num_nodes

    def add_file(self, filename, unique_revs=False):
        with open(filename, "r") as f:
            try:
                fp = Analysis.FileProcessor()
                skip = [False]
                if unique_revs:
                    def after_experiment_callback():
                        skip[0] = True if not self.update_revs(fp.experiment_config) else False
                    fp.after_experiment = after_experiment_callback
                for line in f:
                    fp.handle_event(json.loads(line))
                    if skip[0]:
                        return
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

    def _require_config(self):
        if not self.analysis_config:
            with open(join(dirname(dirname(realpath(__file__))),"config/analyze.json")) as f:
                analysis_json = json.load(f)
                self.analysis_config = {}
                for w in analysis_json["workloads"]:
                    self.analysis_config[w["name"]] = w

    @staticmethod
    def dateformat(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y%m%d %H:%M:%S')

    @staticmethod
    def evalexpr(expr, stats):
        def frac_values(e):
            if e["numer"] in stats and e["denom"] in stats:
                return float(stats[e["numer"]]) / float(stats[e["denom"]])
            else:
                return None
        def value(e):
            return stats[e["value"]] if e["value"] in stats else None
        exprs = {
            "frac_values" : frac_values,
            "value": value
        }
        return exprs[expr["expression_type"]](expr)

    class TableBuilder(object):
        def __init__(self):
            self.rows = []
            self.colnames = []
            self.current_row_colnames = None
            self.current_row_values = None

        def start_row(self):
            if self.current_row_colnames != None or self.current_row_values != None:
                raise RuntimeError("Started row must be finished")
            self.current_row_colnames = []
            self.current_row_values = []

        def add_col(self, name, value):
            self.current_row_colnames.append(name)
            self.current_row_values.append(value)

        def finish_row(self):
            if self.current_row_colnames:
                if not self.colnames:
                    self.colnames = self.current_row_colnames
                elif self.current_row_colnames != self.colnames:
                    raise RuntimeError("inconsistent column names: {} is not {}".format(self.current_row_colnames, self.colnames))
                self.rows.append(self.current_row_values)
            self.current_row_colnames = None
            self.current_row_values = None

        def get_table(self):
            return self.rows, self.colnames

    def workload_trend_key_metrics(self):
        self._require_config()

        def keyfn(s):
            return self.stat_basic_info(s["experiment_config"])

        stat_groups = {k : list(v) for k, v in groupby(sorted(self.all_stats, key=keyfn), key=keyfn)}

        for k in sorted(stat_groups.keys()):
            name, num_workers, num_nodes = k

            print("\n==================================================================================================================\n")
            print("workload:", name)
            print("num workers:", num_workers)
            print("num nodes:", num_nodes)

            if name in self.analysis_config and "key_metrics" in self.analysis_config[name]:
                description = self.analysis_config[name]["description"]
                key_metrics = self.analysis_config[name]["key_metrics"]
                print(description)
                # print(key_metrics)

                std_colnames = ["timestamp", "ray rev", "bm rev", "workload", "workers", "nodes"]
                colnames = []
                rows = []

                def add_col(name, stat):
                    include_col = filter_col(name)
                    if include_col:
                        row.append(stat)
                        row_names.append(include_col)

                tb = self.TableBuilder()
                for s in stat_groups[k]:
                    tb.start_row()

                    timestamp = s["timestamp"]
                    tb.add_col("timestamp", Analysis.dateformat(timestamp))

                    if "git-revs" in s["experiment_config"]:
                        ray_git_rev = s["experiment_config"]["git-revs"]["ray"][:7]
                        benchmark_git_rev = s["experiment_config"]["git-revs"]["benchmark"][:7]
                    else:
                        ray_git_rev = None
                        benchmark_git_rev = None
                    tb.add_col("ray rev", ray_git_rev)
                    tb.add_col("bm rev", benchmark_git_rev)

                    available_stats = {}
                    keyprefixlen = len("None:None:")
                    for key, stats in s["summary_stats"].items():
                        measurement = key[keyprefixlen:]
                        for stat in [ "min_elapsed_time", "max_elapsed_time", "avg_elapsed_time", "ct" ]:
                            stat_name = "{}_{}".format(measurement, stat)
                            available_stats[stat_name] = stats[stat]
                        available_stats["max_successful_iteration"] = s["max_success_iteration"]
                        available_stats["failures_ct"] = s["fail_ct"]
                        for k, v in s["max_success_workload_config"].items():
                            available_stats[k] = v
                        # available_stats["num_tasks"] = s["max_success_workload_config"]["num_tasks"] if "num_tasks" in s["max_success_workload_config"] else None
                    # for name, value in available_stats.items():

                    # print(s)
                    # print(available_stats)
                    for m in key_metrics:
                        value = Analysis.evalexpr(m["expression"], available_stats)
                        tb.add_col(m["name"], value)

                    tb.finish_row()


                # add_col("success_iteration_max", s["max_success_iteration"])
                # add_col("failures_ct", s["fail_ct"])


                rows, colnames = tb.get_table()
                list.sort(rows)
                # print("here are the metrics")
                print(tabulate(rows, headers=colnames, floatfmt=".3f"))

            else:
                print("no key metrics for {}".format(name))


    def workload_trend(self):

        self._require_config()

        def keyfn(s):
            return self.stat_basic_info(s["experiment_config"])

        stat_groups = {k : list(v) for k, v in groupby(sorted(self.all_stats, key=keyfn), key=keyfn)}

        for k in sorted(stat_groups.keys()):
            name, num_workers, num_nodes = k
            filter_col = lambda x: x
            if name in self.analysis_config:
                description = self.analysis_config[name]["description"]
                if "cols" in self.analysis_config[name]:
                    cols = self.analysis_config[name]["cols"]
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
                row = [Analysis.dateformat(timestamp), ray_git_rev, benchmark_git_rev, name, num_workers, num_nodes]
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
    parser.add_argument("--unique-revs", action="store_true", help="output one result per revision")
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
        a.add_file(f, args.unique_revs)
    if args.human_readable:
        print("**KEY METRICS**\n")
        a.workload_trend_key_metrics()
        print("\n**MORE DETAIL**\n")
        a.workload_trend()
    else:
        a.summary_table()
