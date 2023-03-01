import csv
import sys

# This script is to build Figure-4b based on JOB workload
# Terminologies:

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts/run_subplan_statistics.py\n \
\t Script requires 0 argument\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 1:
    print("Wrong number of arguments.\n")
else:
    try:
        cardinality_job_file = "input_data/job/results_estimates-JOB.csv"
        output_f_file = "output_data/job/res_subplan_statistics.csv"

        ##################################### Parsing Cardinalities ######################################################

        all_info = {}
        with open(cardinality_job_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                join_size = int(line[1])
                if join_size not in all_info: all_info[join_size] = 0
                all_info[join_size] += 1

        ##################################################################################################################

        output_f = open(output_f_file, "w")
        output_f_writer = csv.writer(output_f, delimiter=',')
        output_f_writer.writerow(["join size", "number of estimates"])

        for join_size in all_info:
            output_f_writer.writerow([join_size, all_info[join_size]])

        output_f.close()
        print("The results will be saved at: " + output_f_file)
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
