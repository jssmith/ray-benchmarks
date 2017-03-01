import raybench.eventstats as eventstats
import json

from os import listdir
from os.path import join


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

        def handle_event(self, e):
            event_type = e["type"]
            if event_type in self.handlers:
                self.handlers[event_type](e)

        def handle_experiment_event(self, e):
            self.experiment_config = e["data"]

        def handle_finish_work_event(self, e):
            if e["data"]["success"]:
                [self.analysis.add_event(ev) for ev in e["data"]["stats"]["events"]]

        def get_analysis(self):
            a = {}
            a["summary_stats"] = self.analysis.summary_stats()
            a["experiment_config"] = self.experiment_config
            return a

    def add_file(self, filename):
        # print "analyze", filename
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


if __name__ == "__main__":
    experiment_name = "run1"
    log_directory = "./logs"
    a = Analysis()
    for f in listdir(log_directory):
        a.add_file(join(log_directory, f))
    a.summarize()
