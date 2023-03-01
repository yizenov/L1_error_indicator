import csv
import sys
import os

# This script is to build Figure-5b based on JOB workload
# Terminologies:

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts/run_query_complexity.py\n \
\t Script requires 0 argument\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 1:
    print("Wrong number of arguments.\n")
else:
    try:
        input_queries = "input_data/job/JOB_QUERIES_COMPASS_PostgreSQL/"
        cardinality_job_file = "input_data/job/results_estimates-JOB.csv"
        output_f_file = "output_data/job/res_query_complexity.csv"

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

                query_family = int(file_name[2:].split(".")[0][:-1])
                query_complexities[query_family] = len(join_predicates)

        ##################################### Parsing Cardinalities ######################################################
        
        pre_all_info = {}
        with open(cardinality_job_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                query = line[0].split("_")[0]
                if query not in pre_all_info: pre_all_info[query] = 0
                pre_all_info[query] += 1

        all_info, all_counts = set(), {}
        for query in pre_all_info:
            query_family = int(query[:-1])

            all_info.add(query_family)
            if query_family not in all_counts: all_counts[query_family] = pre_all_info[query]

        ##################################################################################################################

        output_f = open(output_f_file, "w")
        output_f_writer = csv.writer(output_f, delimiter=',')
        output_f_writer.writerow(["query family", "query complexity", "simple", "moderate", "complex", "number of estimates"])

        for query_family in sorted(all_info):          
            subplan_count = all_counts[query_family]
            max_join = query_complexities[query_family]

            if max_join < 10: input_row = [query_family, 0, subplan_count, "", "", subplan_count]
            elif 9 < max_join < 20: input_row = [query_family, 1, "", subplan_count, "", subplan_count]
            else: input_row = [query_family, 2, "", "", subplan_count, subplan_count]

            output_f_writer.writerow(input_row)

        output_f.close()
        print("The results will be saved at: " + output_f_file)
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
