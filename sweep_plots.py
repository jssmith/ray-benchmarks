import json
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from itertools import groupby
from cycler import cycler


color_sequence = ['orange', 'green', 'blue', 'cyan', 'red']

def plot_all(data):

    def key_plot(x):
        return (x['config']['benchmark_name'], x['config']['input_file_base'])

    def key_series(x):
        return x['config']['benchmark_implementation']

    serial_series='1-thread'
    all_series = set([key_series(x) for x in data])
    compared_series = all_series - set([serial_series])

    output_filename = 'plots/sweep.pdf'
    with PdfPages(output_filename) as pdf:
        for (k, plot_data) in groupby(data, key_plot):
            plot_data = list(plot_data)

            title = '{} {}'.format(k[0], k[1])
            fig = plt.figure()
            ax = fig.add_subplot(111)            
            ax.set_xlabel('Number of Workers')
            ax.set_ylabel('Job Completion Time [seconds]')
            ax.set_title(title)
            plt.xlim((0,64))

            legend_labels = []
            for i, series in enumerate(all_series):
                series_data = [x for x in plot_data if key_series(x) == series]
                # print "series", series, "has length", len(series_data)
                measured_time = []
                for run in series_data:
                    config = run['config']
                    timing = run['timing']
                    if 'measure:measure:benchmark:measure' in timing:
                        avg_elapsed_time = timing['measure:measure:benchmark:measure']['avg_elapsed_time']
                        measured_time.append((config['num_inputs'], avg_elapsed_time))
                    else:
                        print "timing measurement not found"
                # print "plot ", series, measured_time
                if measured_time:
                    ax.plot([t[0] for t in measured_time], [t[1] for t in measured_time],
                        color=color_sequence[i], marker='o')
                    legend_labels.append(series)
            ax.legend(legend_labels, loc=2)
            pdf.savefig(fig)
            plt.close(fig)

            # plot the speedups
            title = '{} {}'.format(k[0], k[1])
            fig = plt.figure()
            ax = fig.add_subplot(111)            
            ax.set_xlabel('Number of Workers')
            ax.set_ylabel('Speedup Factor')
            ax.set_title(title)
            plt.xlim((0,64))

            ref_data = [x for x in plot_data if key_series(x) == serial_series]
            ref_times = {}
            for run in ref_data:
                config = run['config']
                timing = run['timing']
                if 'measure:measure:benchmark:measure' in timing:
                    avg_elapsed_time = timing['measure:measure:benchmark:measure']['avg_elapsed_time']
                    ref_times[config['num_inputs']] = avg_elapsed_time

            legend_labels = []
            for i, series in enumerate(all_series):
                series_data = [x for x in plot_data if key_series(x) == series]
                # print "series", series, "has length", len(series_data)
                relative_time = []
                for run in series_data:
                    config = run['config']
                    num_inputs = config['num_inputs']
                    timing = run['timing']
                    if 'measure:measure:benchmark:measure' in timing:
                        avg_elapsed_time = timing['measure:measure:benchmark:measure']['avg_elapsed_time']
                        if num_inputs in ref_times:
                            relative_time.append((num_inputs, ref_times[num_inputs] / avg_elapsed_time))
                    else:
                        print "timing measurement not found"
                # print "plot ", series, measured_time
                if relative_time:
                    ax.plot([t[0] for t in relative_time], [t[1] for t in relative_time],
                        color=color_sequence[i], marker='o')
                    legend_labels.append(series)
            ax.legend(legend_labels, loc=2)
            pdf.savefig(fig)
            plt.close(fig)




if __name__ == '__main__':
    with open('sweep_log.json') as f:
        stats = [json.loads(line) for line in f.readlines()]
        plot_all(stats)