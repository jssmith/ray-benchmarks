import json
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from itertools import groupby
from cycler import cycler

# def plot_byworkers(data,
#     x_range,
#     y_variable_description,
#     pdf, title=None):

#     times = map(lambda x: x[0], data)
#     values = map(lambda x: x[1], data)

#     fig = plt.figure()
#     ax = fig.add_subplot(111)
#     ax.plot(times, values, linewidth=1.0)
#     ax.set_xlabel('Number of Workers')
#     ax.set_ylabel(y_variable_description)
#     if title is not None:
#         ax.set_title(title)
#     plt.xlim(x_range)
#     pdf.savefig(fig)
#     plt.close(fig)

# def plot_duration_breakdown(data, series_labels, title, pdf):
#     fig = plt.figure()
#     ax = fig.add_subplot(111)
#     num_workers_labels = map(lambda x: x[0], data)
#     barl = range(len(num_workers_labels))
#     color_cycle = ['r', 'g', 'b', 'y', 'm']
#     bars = []
#     for i in range(len(series_labels)):
#         zz = map(lambda x: x[1][i], data)
#         print barl, zz
#         bars.append(ax.bar(barl, zz, 0.8, color=color_cycle[i]))
#     plt.legend(map(lambda x: x[0], bars), series_labels, loc=2)
#     plt.title(title)
#     pdf.savefig(fig)
#     plt.close(fig)

color_sequence = ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c',
                  '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5',
                  '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f',
                  '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5']

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
                p = ax.plot([t[0] for t in measured_time], [t[1] for t in measured_time],
                    color=color_sequence[i], marker='o')
            ax.legend(all_series, loc=2)
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
                        relative_time.append((num_inputs, ref_times[num_inputs] / avg_elapsed_time))
                    else:
                        print "timing measurement not found"
                # print "plot ", series, measured_time
                p = ax.plot([t[0] for t in relative_time], [t[1] for t in relative_time],
                    color=color_sequence[i], marker='o')
            ax.legend(all_series, loc=2)
            pdf.savefig(fig)
            plt.close(fig)




if __name__ == '__main__':
    with open('sweep_log.json') as f:
        stats = [json.loads(line) for line in f.readlines()]
        plot_all(stats)