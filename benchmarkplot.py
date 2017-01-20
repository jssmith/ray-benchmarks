import argparse
import gzip
import json
from os import listdir
from os.path import isfile, join

from plot_worker_activity import DistributionStats, plot_worker_activity

from matplotlib.backends.backend_pdf import PdfPages

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot Ray workloads")
    parser.add_argument("--log-directory", required=True, help="log directory")
    args = parser.parse_args()

    files = [f for f in listdir(args.log_directory) if f.endswith(".json.gz") and isfile(join(args.log_directory, f))]

    for input_file in [join(args.log_directory, f) for f in files]:
        with gzip.open(input_file) as f:
            dump = json.load(f)
        # print dump
        ds = DistributionStats()
        for e in dump['events']:
            ds.add_event(e)
        stats = ds.get_stats()
        print dump['config']

        output_filename = input_file[:-8] + ".pdf"

        with PdfPages(output_filename) as pdf:
            plot_worker_activity(stats['worker_activity'], dump['worker_ips'], output_filename, pdf)
