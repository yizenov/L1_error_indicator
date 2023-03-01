import csv
import sys
import os

# This script is to build Figure-5a based on PostgreSQL, Exhaustive and Greedy Searches, JOB workload
# Terminologies: optimal (opt), PostgreSQL (psql)

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts/run_compare_enumerators.py\n \
\t Script requires 0 argument\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 1:
    print("Wrong number of arguments.\n")
else:
    try:
        input_queries = "input_data/job/JOB_QUERIES_COMPASS_PostgreSQL/"
        output_f_file = "output_data/job/res_compare_enumerators.csv"

        opt_exhaustive_plan_cost_file = "input_data/job/exhaustive_traversals-opt-cost.csv"
        psql_exhaustive_plan_cost_file = "input_data/job/exhaustive_traversals-opt-cost-psql.csv"
        opt_greedy_plan_cost_file = "input_data/job/greedy_traversals-opt-cost.csv"
        psql_greedy_plan_cost_file = "input_data/job/greedy_traversals-opt-cost-psql.csv"

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

        ##################################### Optimal Exhaustive Plans ##################################################

        opt_exhaustive_plan_costs = {}
        with open(opt_exhaustive_plan_cost_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(',')
                query = line[0]

                cost = int(line[2])
                opt_exhaustive_plan_costs[query] = cost

        ##################################### PSQL Exhaustive Plans #####################################################

        psql_exhaustive_plan_costs = {}
        with open(psql_exhaustive_plan_cost_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(',')
                query = line[0]

                cost = int(line[2])
                psql_exhaustive_plan_costs[query] = cost

        ##################################### Optimal Greedy Plans ######################################################

        opt_greedy_plan_costs = {}
        with open(opt_greedy_plan_cost_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(',')
                query = line[0]

                cost = int(line[2])
                opt_greedy_plan_costs[query] = cost

        ##################################### PSQL Greedy Plans ##########################################################

        psql_greedy_plan_costs = {}
        with open(psql_greedy_plan_cost_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(',')
                query = line[0]

                cost = int(line[2])
                psql_greedy_plan_costs[query] = cost

        ##################################################################################################################

        output_f = open(output_f_file, "w")
        output_f_writer = csv.writer(output_f, delimiter=',')

        output_f_writer.writerow(["query_name", "tables_nbr", "query_complexity", 
            "optimal greedy", "psql greedy", "optimal exhaustive", "psql exhaustive"])

        group1_queries, group2_queries, group3_queries = [], [], []
        for query in query_complexities:
            max_join = query_complexities[query]
            if max_join < 10: group1_queries.append(query)
            elif 9 < max_join < 20: group2_queries.append(query)
            else: group3_queries.append(query)

        for group_idx, group in enumerate([group1_queries, group2_queries, group3_queries]):
            for query in group:
                max_join = query_complexities[query]

                output_f_writer.writerow([query, max_join, group_idx, 
                    opt_greedy_plan_costs[query], 
                    psql_greedy_plan_costs[query],
                    opt_exhaustive_plan_costs[query],
                    psql_exhaustive_plan_costs[query]
                ])

        output_f.close()
        print("The results will be saved at: " + output_f_file)
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
