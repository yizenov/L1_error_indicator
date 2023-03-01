import sys
import csv
import os

# This script is to build Figure-9 based on PostgreSQL, Greedy Search, JOB workload
# Terminologies: optimal (opt), PostgreSQL (psql)

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts/run_perror_distribution.py\n \
\t Script requires 0 argument\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 1:
    print("Wrong number of arguments.\n")
else:
    try:
        input_queries = "input_data/job/JOB_QUERIES_COMPASS_PostgreSQL/"
        opt_plan_cost_file = "input_data/job/greedy_traversals-opt-cost.csv"
        psql_plans_file = "input_data/job/greedy_traversals-opt-cost-psql.csv"
        output_f_file = "output_data/job/res_perror_distribution.csv"

        ##################################### Original Queries ###########################################################

        query_complexities = {}
        for file_name in sorted(os.listdir(input_queries)):
            input_query = input_queries + file_name
            with open(input_query, "r") as query_input_f:
                original_query = [query_line for query_line in query_input_f]
                original_query = "".join(original_query)

                # extracting tables and join predicates
                from_and_where = original_query.split('FROM')[1].split('WHERE')
                where_clause = from_and_where[1].split('\n\n')
                where_clause = [clause_set for clause_set in where_clause if clause_set]

                join_predicates = where_clause[1].split('AND')
                join_predicates = [join.strip() for join in join_predicates if join.strip()]
                join_predicates[-1] = join_predicates[-1][:-1]

                query = file_name[2:].split(".")[0]
                query_complexities[query] = len(join_predicates)

        ##################################### Optimal Plans #################################################################

        opt_plan_costs = {}
        with open(opt_plan_cost_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(',')

                query, true_cost = line[0], float(line[2])
                opt_plan_costs[query] = true_cost

        ################################## PSQL plans #######################################################################

        psql_plan_costs = {}
        with open(psql_plans_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                query, true_cost = line[0], float(line[2])
                psql_plan_costs[query] = true_cost

        ######################################################################################################################
        
        output_f = open(output_f_file, "w")
        output_f_writer = csv.writer(output_f, delimiter=',')

        output_f_writer.writerow(["ratio range", "query counts", "percents", 
            "simple query", "moderate query", "complex query"])

        all_info = [
            ["(0, 1)", 0, 0, 0, 0, 0],
            ["1", 0, 0, 0, 0, 0],
            ["(1, 1.5)", 0, 0, 0, 0, 0],
            ["[1.5, 2)", 0, 0, 0, 0, 0],
            ["[2, 3)", 0, 0, 0, 0, 0],
            ["[3, 5)", 0, 0, 0, 0, 0],
            ["[5, 10)", 0, 0, 0, 0, 0],
            ["[10, 100)", 0, 0, 0, 0, 0],
            ["[100, inf+)", 0, 0, 0, 0, 0]
        ]

        for query in opt_plan_costs:
            true_cost = opt_plan_costs[query]
            psql_cost = psql_plan_costs[query]
            ratio = psql_cost / true_cost

            ratio_range_idx = -1
            if ratio < 1: ratio_range_idx = 0
            elif ratio == 1: ratio_range_idx = 1
            elif 1 < ratio < 1.5: ratio_range_idx = 2
            elif 1.5 <= ratio < 2: ratio_range_idx = 3
            elif 2 <= ratio < 3: ratio_range_idx = 4
            elif 3 <= ratio < 5: ratio_range_idx = 5
            elif 5 <= ratio < 10: ratio_range_idx = 6
            elif 10 <= ratio < 100: ratio_range_idx = 7
            elif 100 <= ratio: ratio_range_idx = 8

            max_join = query_complexities[query]
            if max_join < 10: update_idx = 3
            elif 9 < max_join < 20: update_idx = 4
            else: update_idx = 5

            all_info[ratio_range_idx][1] += 1
            all_info[ratio_range_idx][update_idx] += 1

        total_queries, prev_count = len(query_complexities), 0
        for row in all_info:
            row[2] = row[1] * 100 / total_queries + prev_count
            prev_count = row[2]
            output_f_writer.writerow(row)

        output_f.close()
        print("The results will be saved at: " + output_f_file)
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
