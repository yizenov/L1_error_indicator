import sys
import statistics

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.font_manager as font_manager

# This script is to build Figure-4a based on PostgreSQL, JOB workload
# Terminologies:

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts/run_evaluate_q_error.py\n \
\t Script requires 0 argument\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 1:
    print("Wrong number of arguments.\n")
else:
    try:
        cardinality_job_file = "input_data/job/results_estimates-JOB.csv"
        output_f_file = "output_data/job/res_evaluate_q_error.pdf"
        ZERO_REPLACE = 10**(-4)

        ##################################### Parsing Cardinalities ######################################################

        all_info = {}
        with open(cardinality_job_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                join_size = int(line[1])
                true_cardinality = float(line[2])
                estimate = float(line[3])

                q_error_curr = max([
                    estimate / max(true_cardinality, ZERO_REPLACE), 
                    true_cardinality / max(estimate, ZERO_REPLACE)
                ])

                if join_size not in all_info: all_info[join_size] = []
                all_info[join_size].append(q_error_curr)

        ##################################### Parsing Cardinalities ######################################################

        box_plot_data = []
        for join_size in sorted(all_info):

            box_values = sorted(all_info[join_size])
            five_idx = int(0.05 * len(box_values))
            two_five_idx = int(0.25 * len(box_values))
            seven_five_idx = int(0.75 * len(box_values))
            nine_five_idx = int(0.95 * len(box_values))

            box_plot_data.append([join_size, box_values, 
                box_values[five_idx], box_values[two_five_idx], statistics.median(box_values),
                box_values[seven_five_idx], box_values[nine_five_idx], five_idx, nine_five_idx])

        #############################################################################################

        figure, axes = plt.subplots(nrows=1, ncols=1, figsize=(6, 6))
        boxes = []
        for idx, stats in enumerate(box_plot_data):
            # labels, bottom-whisker, first-percentile, median, second-percentile, top-whisker, outliers
            data = {'label': str(stats[0]), 'whislo': stats[2], 'q1': stats[3],
                    'med': stats[4], 'q3': stats[5], 'whishi': stats[6],
                    'fliers': stats[1][:stats[7]] + stats[1][stats[8]:]}
            boxes.append(data)
        boxprops, medianprops = dict(linewidth=2), dict(linewidth=2.5)
        flierprops = dict(marker='o', markersize=1, linestyle='none')

        figure.tight_layout()
        axes.margins(tight=True)
        plt.subplots_adjust(left=0.095, right=0.99, bottom=0.075, top=0.99)

        axes.tick_params(axis='both', labelsize=10)
        axes.set_ylim(bottom=0.8, top=1500000000)

        axes.bxp(boxes, showfliers=True, boxprops=boxprops, flierprops=flierprops, medianprops=medianprops)
        axes.set_yscale('log')
        font_size = 11
        axes.set_ylabel("Q-error (log scale)", fontsize=font_size, fontdict=dict(weight='bold'))
        axes.set_xlabel("join size", fontsize=font_size, fontdict=dict(weight='bold'))
        from matplotlib.lines import Line2D
        handles = [
            Line2D([0], [0], color='black', lw=1, label="95th percentile"),
            Line2D([0], [0], color='black', lw=2, label="75th percentile"),
            Line2D([0], [0], color='orange', lw=3, label="median"),
            Line2D([0], [0], color='black', lw=2, label="25th percentile"),
            Line2D([0], [0], color='black', lw=1, label="5th percentile"),
            Line2D([0], [0], marker='o', color='w', label='outliers',
                markerfacecolor='black', markersize=5)
        ]
        font = font_manager.FontProperties(weight='bold', style='normal', size=font_size)
        axes.legend(handles=handles, frameon=False, loc='upper left', bbox_to_anchor=(0.0, 1.01), fontsize=font_size, prop=font)
        axes.grid(True)
        plt.show()

        q_error_overall = PdfPages(output_f_file)
        q_error_overall.savefig(figure)
        q_error_overall.close()

        print("The results will be saved at: " + output_f_file)
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
