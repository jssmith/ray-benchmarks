import argparse
import raybench.eventstats as eventstats
import json

from os import listdir
from os.path import join, isdir


class Analysis(object):
    def __init__(self):
        self.all_stats = []

    class FileProcessor(object):
        def __init__(self):
            self.handlers = {
                "experiment" : self.handle_experiment_event,
                "finish_work" : self.handle_finish_work_event
            }
            self.experiment_config = None
            self.analysis = eventstats.Analysis()
            self.fail_ct = 0
            self.max_success_iteration = 0

        def handle_event(self, e):
            event_type = e["type"]
            if event_type in self.handlers:
                self.handlers[event_type](e)

        def handle_experiment_event(self, e):
            self.experiment_config = e["data"]

        def handle_finish_work_event(self, e):
            if e["data"]["success"]:
                [self.analysis.add_event(ev) for ev in e["data"]["stats"]["events"]]
                if e["data"]["iteration"] > self.max_success_iteration:
                    self.max_success_iteration = e["data"]["iteration"]
            else:
                self.fail_ct += 1

        def get_analysis(self):
            a = {}
            a["summary_stats"] = self.analysis.summary_stats()
            a["fail_ct"] = self.fail_ct
            a["max_success_iteration"] = self.max_success_iteration
            a["experiment_config"] = self.experiment_config
            return a

    def add_file(self, filename):
        with open(filename, "r") as f:
            fp = Analysis.FileProcessor()
            for line in f:
                fp.handle_event(json.loads(line))
            self.all_stats.append(fp.get_analysis())

    def summarize(self):
        stat_names = { "min_elapsed_time" : "min",
            "max_elapsed_time" : "max",
            "avg_elapsed_time" : "avg",
            "ct" : "ct"
            }
        for s in self.all_stats:
            sc = s["experiment_config"]["system_config"]
            wl = s["experiment_config"]["workload"]
            name = wl["name"]
            num_workers = sc["num_workers"]
            num_nodes = sc["num_nodes"]
            experiment_desc = "{}_{}_{}".format(name, num_workers, num_nodes)
            keyprefixlen = len("None:None:")
            for key, stats in s["summary_stats"].items():
                measurement = key[keyprefixlen:]
                for stat in [ "min_elapsed_time", "max_elapsed_time", "avg_elapsed_time", "ct" ]:
                    print name, num_workers, num_nodes, measurement, stat_names[stat], stats[stat]
            print name, num_workers, num_nodes, "success_iteration", "max", s["max_success_iteration"]
            print name, num_workers, num_nodes, "failures", "ct", s["fail_ct"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="analyze.py", description="Analyze logs of Ray performance and stress testing")
    parser.add_argument("logdirectory", help="json configuration file")
    parser.add_argument("--no-recurse", action="store_true", help="look for log files recursively")
    args = parser.parse_args()

    def findfiles(dir):
        all_files = []
        for name in listdir(dir):
            name_path = join(dir, name)
            if isdir(name_path):
                if not args.no_recurse:
                    all_files += findfiles(name_path)
            else:
                all_files.append(name_path)
        return all_files

    a = Analysis()
    for f in findfiles(args.logdirectory):
        a.add_file(f)
    a.summarize()
