import os
import csv
import sys
import statistics

# This script is to build Figure-7 based on JOB workload
# Terminologies:

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts/run_join_size_analysis.py\n \
\t Script requires 0 argument\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 1:
    print("Wrong number of arguments.\n")
else:
    try:
        input_queries = "input_data/job/JOB_QUERIES_COMPASS_PostgreSQL"
        cardinality_job_file = "input_data/job/results_estimates-JOB.csv"
        output_f_file = "output_data/job/res_join_size_analysis.csv"

        ##################################### Original Queries ###########################################################

        query_predictes_number = {}
        for f_idx, file_name in enumerate(sorted(os.listdir(input_queries))):
            query = file_name[2:-4]
            input_query = input_queries + "/" + file_name
            with open(input_query, "r") as query_input_f:

                original_query = [query_line for query_line in query_input_f]
                original_query = "".join(original_query).strip()

                from_and_where = original_query.split('FROM')[1].split('WHERE')
                table_list = from_and_where[0].split(',')
                table_list = [table.strip() for table in table_list]
                where_clause = from_and_where[1].split('\n\n')
                where_clause = [clause_set for clause_set in where_clause if clause_set]

                join_predicates = where_clause[1].split('AND')
                join_predicates = [join.strip() for join in join_predicates if join.strip()]
                join_predicates[-1] = join_predicates[-1][:-1]

                if query not in query_predictes_number: query_predictes_number[query] = [0, 0]
                query_predictes_number[query][0] = max(len(join_predicates), query_predictes_number[query][0])
                query_predictes_number[query][1] = max(len(table_list), query_predictes_number[query][1])

        ##################################### Parsing Cardinalities ######################################################

        all_true_cardinalities = {}
        with open(cardinality_job_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                query, join_size = line[0].split("_")[0], int(line[1])
                subquery, true_card = line[0], int(line[2])

                if query not in all_true_cardinalities: all_true_cardinalities[query] = {}
                all_true_cardinalities[query][subquery] = [true_card, join_size]

        ##################################################################################################################

        simple_queries, simple_count = {}, 0
        moderate_queries, moderate_count = {}, 0
        complex_queries, complex_count = {}, 0

        max_join, max_predicate = 0, 0
        min_join, min_predicate = 100, 100
        for idx, query in enumerate(query_predictes_number):
            max_predicates_size, max_join_size = query_predictes_number[query]

            max_join = max(max_join, max_join_size)
            min_join = min(min_join, max_join_size)
            max_predicate = max(max_predicate, max_predicates_size)
            min_predicate = min(min_predicate, max_predicates_size)

            if 3 < max_predicates_size < 10:
                simple_count += 1
                for sub_query in all_true_cardinalities[query]:
                    true_card, join_size = all_true_cardinalities[query][sub_query]
                    if join_size not in simple_queries: simple_queries[join_size] = []
                    simple_queries[join_size].append(true_card)
            elif 9 < max_predicates_size < 20:
                moderate_count += 1
                for sub_query in all_true_cardinalities[query]:
                    true_card, join_size = all_true_cardinalities[query][sub_query]
                    if join_size not in moderate_queries: moderate_queries[join_size] = []
                    moderate_queries[join_size].append(true_card)
            elif 19 < max_predicates_size < 29:
                complex_count += 1
                for sub_query in all_true_cardinalities[query]:
                    true_card, join_size = all_true_cardinalities[query][sub_query]
                    if join_size not in complex_queries: complex_queries[join_size] = []
                    complex_queries[join_size].append(true_card)

        print("\ntotal: queries: " + str([len(all_true_cardinalities), len(query_predictes_number)]))
        print("min join and max join: " + str([min_join, max_join]))
        print("predicates range: " + str([min_predicate, max_predicate]))
        print("simple, moderate and complex queries: " + str([simple_count, moderate_count, complex_count]))
        print("simple, moderate and complex max joins: " + str([len(simple_queries), len(moderate_queries), len(complex_queries)]))

        output_f = open(output_f_file, "w")
        output_f_writer = csv.writer(output_f, delimiter=',')
        output_f_writer.writerow(["join_size", 
            "simple_complexity", "simple_count", "simple_median",
            "moderate_complexity", "moderate_count", "moderate_median",
            "complex_complexity", "complex_count", "complex_median"
        ])

        for join_size in range(2, max_join_size + 1):
            if join_size in simple_queries and join_size in moderate_queries and join_size in complex_queries:
                simple_count = len(simple_queries[join_size])
                simple_median = round(statistics.median(simple_queries[join_size]), 2)
                moderate_count = len(moderate_queries[join_size])
                moderate_median = round(statistics.median(moderate_queries[join_size]), 2)
                complex_count = len(complex_queries[join_size])
                complex_median = round(statistics.median(complex_queries[join_size]), 2)
                data_row = [join_size, 0, simple_count, simple_median, 
                            1, moderate_count, moderate_median, 
                            2, complex_count, complex_median]
            elif join_size in moderate_queries and join_size in complex_queries:
                moderate_count = len(moderate_queries[join_size])
                moderate_median = round(statistics.median(moderate_queries[join_size]), 2)
                complex_count = len(complex_queries[join_size])
                complex_median = round(statistics.median(complex_queries[join_size]), 2)
                data_row = [join_size, "", "", "", 
                            1, moderate_count, moderate_median, 
                            2, complex_count, complex_median]
            elif join_size in complex_queries:         
                complex_count = len(complex_queries[join_size])
                complex_median = round(statistics.median(complex_queries[join_size]), 2)
                data_row = [join_size, "", "", "", "", "", "", 
                            2, complex_count, complex_median]
            output_f_writer.writerow(data_row)

        output_f.close()
        print("\nThe results will be saved at: " + output_f_file)
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
