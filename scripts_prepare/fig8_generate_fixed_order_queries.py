import sys
import os

# This script is to generate all sql plan files for Figure-8 based on Exhaustive Search, JOB workload
# Terminologies:

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts_prepare/fig8_generate_fixed_order_queries.py arg1\n \
\t Script requires 1 argument: JOB query name\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 2:
    print("Wrong number of arguments.\n")
else:
    try:
        needed_query_name = sys.argv[1]
        needed_queries = [needed_query_name]

        query_files = "input_data/job/JOB_QUERIES_COMPASS_PostgreSQL"
        plan_file_name = "input_data/job/exhaustive_all_plans_with_costs.csv"
        output_folder = "input_data/job/FIXED_PLAN_JOB_EXHAUSTIVE_OPT"

        #############################################################################################

        all_plans = []
        with open(plan_file_name, "r") as input_f:  # only left-deep plans
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                query, query_idx = line[0].split("_")[0], line[0].split("_")[1]
                plan_size, true_cost = line[1], float(line[2])

                plan = ",".join(line[3:])[2:-2].split(",")
                plan = [node.strip()[1:-1] for node in plan]

                all_plans.append([plan, true_cost, query_idx])

        ######################## Generate Queries ##################################################

        input_query_files = sorted(os.listdir(query_files))
        for in_query_id in range(len(all_plans)):
            query_plan, query_true_cost = all_plans[in_query_id][0], all_plans[in_query_id][1]
            query_idx = all_plans[in_query_id][2]

            for idx, file_name in enumerate(input_query_files):
                query_name = file_name.split("_")[1][:-4]
                if query_name != query: continue

                input_query = query_files + "/" + file_name
                with open(input_query, "r") as query_input_f:

                    original_query = [query_line for query_line in query_input_f]
                    original_query = "".join(original_query)

                    # extracting tables and join predicates
                    from_and_where = original_query.split('FROM')[1].split('WHERE')
                    table_list = from_and_where[0].split(',')
                    table_list = [table.strip().split(" AS ") for table in table_list]
                    table_list = {table_info[1]: table_info[0] for table_info in table_list}
                    [query_plan.append(t_id) for t_id in table_list if t_id not in query_plan]

                    # collecting tables and join predicates information
                    where_clause = from_and_where[1].split('\n\n')
                    where_clause = [clause_set for clause_set in where_clause if clause_set]

                    filter_predicates = where_clause[0].split('\n')
                    filter_predicates = [cond.strip() for cond in filter_predicates if cond.strip()]
                    filter_predicates = [cond[4:] if c_idx > 0 else cond for c_idx, cond in enumerate(filter_predicates)]
                    filter_predicates = {cond.split(".")[0]: cond for cond in filter_predicates}
                    
                    join_preds = where_clause[1].split('\n')
                    join_preds = [join.strip() for join in join_preds if join.strip()]
                    join_preds[-1] = join_preds[-1][:-1]
                    join_preds = [cond[4:] for cond in join_preds]
                    join_predicates = {}
                    for cond in join_preds:
                        cond_both = cond.split(" = ")
                        cond_key_v1 = cond_both[0].split(".")[0] + "," + cond_both[1].split(".")[0]
                        cond_key_v2 = cond_both[1].split(".")[0] + "," + cond_both[0].split(".")[0]
                        join_predicates[cond_key_v1] = cond
                        join_predicates[cond_key_v2] = cond
                    
                    joined_tables = {query_plan[0]}
                    new_query = "SELECT COUNT(*) \nFROM " + table_list[query_plan[0]] + " AS " + query_plan[0]
                    for t_id in range(1, len(query_plan)):
                        joined_tables.add(query_plan[t_id])
                        curr_conds, to_delete_indexes = "", []

                        for join in join_predicates:
                            join = join.split(",")
                            join_v1 = join[0] + "," + join[1]
                            join_v2 = join[1] + "," + join[0]
                            if join[0] in joined_tables and join[1] in joined_tables and join_v1 not in to_delete_indexes and join_v2 not in to_delete_indexes:
                                to_delete_indexes.append(join_v1)
                                to_delete_indexes.append(join_v2)
                                curr_conds += join_predicates[join_v1] + " AND "
                        curr_conds = curr_conds[:-5]
                        [join_predicates.pop(join, None) for join in to_delete_indexes]
                        to_delete_indexes.clear()

                        for t_id_filter in filter_predicates:
                            if t_id_filter in joined_tables:
                                to_delete_indexes.append(t_id_filter)
                                curr_conds += " AND " + filter_predicates[t_id_filter]
                        [filter_predicates.pop(join, None) for join in to_delete_indexes]
                        to_delete_indexes.clear()

                        new_query += " \nJOIN " + table_list[query_plan[t_id]] + " AS " + query_plan[t_id] + " ON (" + curr_conds + ")"
                    new_query += ";\n"

                    output_f = open(output_folder + "/fixed_order_" + file_name[:-4] + "_" + query_idx + ".sql", "w")
                    output_f.write(new_query)
                    output_f.close()
        print("\nSuccess.\n")
    except:
        print("Wrong parameter type or code error.\n")
