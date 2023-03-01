import os
import statistics
import csv
import sys

# This script is to build Figure-8 based on PostgreSQL, Exhaustive Search, JOB workload
# Terminologies:

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts/run_cost_runtime.py arg1\n \
\t Script requires 1 argument: runtime executions per query\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 2:
    print("Wrong number of arguments.\n")
else:
    try:
        query_cost_plans = "input_data/job/exhaustive_all_plans_with_costs.csv"
        input_queries = "input_data/job/RESULTS_FIXED_PLAN_JOB_EXHAUSTIVE_OPT"
        output_f_file = "output_data/job/res_cost_runtime.csv"
        NUMBER_OF_RUNS = int(sys.argv[1])  # default 5

        ##################################### Query Costs ##############################################################

        opt_plan_costs = {}
        with open(query_cost_plans, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(',')

                query, query_id = line[0].split("_")[0], int(line[0].split("_")[1])
                plan_size, cost = int(line[1]), int(line[2])
                plan = ",".join(line[3:])[2:-2]
                plan = [node.strip()[1:-1] for node in plan.split(",")]

                if query_id not in opt_plan_costs: 
                    opt_plan_costs[query_id] = [query + "_" + str(query_id), plan_size, cost, plan]
        
        ##################################### Parse Runtimes ##############################################################

        all_data = {}
        for idx, file_name in enumerate(sorted(os.listdir(input_queries))):
            if ".result" not in file_name: continue
            result_file = input_queries + "/" + file_name
            with open(result_file, "r") as input_f:
                runtimes, cardinalities, is_cardinality = [], [], False
                for idx, line in enumerate(input_f):
                    if "Time: " in line:  # in ms
                        runtimes.append(float(line.split(":")[1].split(" ms")[0].strip()))
                    if "-------" in line: 
                        is_cardinality = True
                        continue
                    if is_cardinality:
                        cardinalities.append(line.strip())
                        is_cardinality = False

                if len(runtimes) != NUMBER_OF_RUNS or len(cardinalities) != NUMBER_OF_RUNS: 
                    print(["missing runs, ", file_name])
                for card in cardinalities:
                    if float(card) != float(cardinalities[0]): 
                        print(["different cardinalities, ", file_name])

                query_name = file_name.split(".")[0].split("_")[3]
                query_id = int(file_name.split(".")[0].split("_")[4])
                median_run = statistics.median(runtimes)
                all_data[query_id] = median_run
        all_data = sorted(all_data.items(), key=lambda x: x[1])

        ##################################################################################################################

        output_f = open(output_f_file, "w")
        output_f_writer = csv.writer(output_f, delimiter=',')
        output_f_writer.writerow(["query name", "plan size", "true cost", "plan", "median runtime (ms)"])
        for query_info in all_data:
            query_data = opt_plan_costs[query_info[0]]
            query_data.append(query_info[1])
            output_f_writer.writerow(query_data)
        output_f.close()

        print("The results will be saved at: " + output_f_file)
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
