import os
import csv
import math
import sys

# This script is to prepare exhaustive plans on JOB and JOB-light workload
# Terminologies: optimal (opt), PostgreSQL (psql), COMPASS (compass)

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts_prepare/collect_exhaustive_plans.py arg1\n \
\t Script requires 1 argument\n \
\t Workload: (1 = JOB-light, 0 = JOB)\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 2:
    print("Wrong number of arguments.\n")
else:
    try:
        is_job_light = int(sys.argv[1])

        workload_folder = "input_data/job/JOB_QUERIES_COMPASS_PostgreSQL"
        if is_job_light: workload_folder = "input_data/job_light/JOB_light_QUERIES"

        all_estimate_file = "input_data/job/results_estimates-JOB.csv"
        if is_job_light: all_estimate_file = "input_data/job_light/results_estimates-JOB-light.csv"

        parent_folder = "input_data/job/"
        if is_job_light: parent_folder = "input_data/job_light/"
        output_f_file_opt1 = parent_folder + "exhaustive_traversals-opt-cost.csv"
        output_f_file_opt2 = parent_folder + "exhaustive_traversals-opt-cost-compass.csv"
        output_f_file_opt3 = parent_folder + "exhaustive_traversals-opt-cost-psql.csv"

        output_files = [output_f_file_opt1, output_f_file_opt2, output_f_file_opt3]
        output_file_objects, output_writers = [], []
        for system_id in range(len(output_files)):
            output_f_opt = open(output_files[system_id], "w")
            output_file_objects.append(output_f_opt)
            output_f_writer_opt = csv.writer(output_f_opt, delimiter=',')
            output_f_writer_opt.writerow(["query_name", "plan_size", "true_cost", "compass_est", "psql_est", "plan"])
            output_writers.append(output_f_writer_opt)

        ######################################################################################

        query_sub_plans, query_sub_plans_psql, query_sub_plans_compass = {}, {}, {}
        with open(all_estimate_file, "r") as query_input_f:
            for idx, line in enumerate(query_input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                query = line[0].split("_")[0]
                if query not in query_sub_plans: query_sub_plans[query] = {}
                if query not in query_sub_plans_compass: query_sub_plans_compass[query] = {}
                if query not in query_sub_plans_psql: query_sub_plans_psql[query] = {}

                plan_size = int(line[1])
                true_cardinality = int(line[2])
                psql_est_cardinality = round(float(line[3]))  # psql estimates
                compass_est_cardinality = round(float(line[4]))  # compass estimates

                plan = ",".join(line[5:])[2:-2]
                plan = plan.split(",")
                plan = sorted([node.strip()[1:-1].split("-")[1] for node in plan])

                plan = [" " + node + " " for node in plan]
                plan = " ".join(plan)
                if plan not in query_sub_plans[query]:
                    query_sub_plans[query][plan] = true_cardinality
                    query_sub_plans_compass[query][plan] = compass_est_cardinality
                    query_sub_plans_psql[query][plan] = psql_est_cardinality

        ######################################################################################

        def traverse_opt_plan(query, param_node, param_join_graph, param_visited_nodes,
                            param_traversed_nodes_order, param_all_adjacent_nodes,
                            param_distinct_sub_plans, param_distinct_sub_plans_trues,
                            param_all_sub_plans, param_all_sub_plans_compass, param_all_sub_plans_psql):
            param_visited_nodes.add(param_node)
            param_traversed_nodes_order.append(param_node)

            if len(param_traversed_nodes_order) >= 2:
                sorted_plan_string = " ".join(sorted(param_traversed_nodes_order))
                plan_string = " ".join(param_traversed_nodes_order)

                true_card = round(float(param_all_sub_plans[query][sorted_plan_string]))
                compass_est = round(float(param_all_sub_plans_compass[query][sorted_plan_string]))
                psql_est = round(float(param_all_sub_plans_psql[query][sorted_plan_string]))
                new_str = plan_string + " :: " + str(true_card) + ", " + str(compass_est) + ", " + str(psql_est)

                if len(param_traversed_nodes_order) == 2:
                    param_distinct_sub_plans_trues[plan_string] = \
                        (len(param_traversed_nodes_order),
                        true_card, compass_est, psql_est,
                        [new_str])
                else:
                    temp = " ".join(param_traversed_nodes_order[:-1])
                    param_distinct_sub_plans_trues[plan_string] = \
                        (len(param_traversed_nodes_order),
                        param_distinct_sub_plans_trues[temp][1] + true_card,
                        param_distinct_sub_plans_trues[temp][2] + compass_est,
                        param_distinct_sub_plans_trues[temp][3] + psql_est,
                        [param_distinct_sub_plans_trues[temp][4], new_str])

                if sorted_plan_string not in param_distinct_sub_plans:
                    param_distinct_sub_plans.add(sorted_plan_string)
                    # TODO: can be omitted, it is only printing current stage
                if len(param_traversed_nodes_order) == len(param_join_graph): return

            adj_nodes = [adj for adj in param_join_graph[param_node] if adj not in param_visited_nodes]
            [param_all_adjacent_nodes.add(adj) for adj in adj_nodes]

            for adj in adj_nodes:
                if adj not in param_visited_nodes:
                    local_adj_nodes = set(temp_adj for temp_adj in param_all_adjacent_nodes if temp_adj not in param_visited_nodes and temp_adj != adj)
                    traverse_opt_plan(query, adj, param_join_graph, param_visited_nodes,
                                    param_traversed_nodes_order, local_adj_nodes,
                                    param_distinct_sub_plans, param_distinct_sub_plans_trues,
                                    param_all_sub_plans, param_all_sub_plans_compass, param_all_sub_plans_psql)
                    param_visited_nodes.remove(adj)
                    param_traversed_nodes_order.pop()

            open_adj_nodes = [adj for adj in param_all_adjacent_nodes if adj not in param_visited_nodes and adj not in adj_nodes]
            [param_all_adjacent_nodes.add(adj) for adj in open_adj_nodes]

            for adj in open_adj_nodes:
                if adj not in param_visited_nodes:
                    local_adj_nodes = set(temp_adj for temp_adj in param_all_adjacent_nodes if temp_adj not in param_visited_nodes and temp_adj != adj)
                    traverse_opt_plan(query, adj, param_join_graph, param_visited_nodes,
                                    param_traversed_nodes_order, local_adj_nodes,
                                    param_distinct_sub_plans, param_distinct_sub_plans_trues,
                                    param_all_sub_plans, param_all_sub_plans_compass, param_all_sub_plans_psql)
                    param_visited_nodes.remove(adj)
                    param_traversed_nodes_order.pop()

        ######################################################################################

        for f_idx, file_name in enumerate(sorted(os.listdir(workload_folder))):
            input_query = workload_folder + "/" + file_name
            if is_job_light: query = file_name.split(".")[0]
            else: query = file_name.split(".")[0].split("_")[1]
        
            join_graph, distinct_sub_plans = {}, set()
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

                for join in join_predicates:
                    left = join.split('=')[0].strip()
                    right = join.split('=')[1].strip()
                    left_nick, right_nick = " " + left.split('.')[0] + " ", " " + right.split('.')[0] + " "

                    if left_nick not in join_graph: join_graph[left_nick] = []
                    join_graph[left_nick].append(right_nick)
                    if right_nick not in join_graph: join_graph[right_nick] = []
                    join_graph[right_nick].append(left_nick)

                # key[plan] = [join_size, true_card, compass_est, psql_est, all]
                distinct_sub_plans_trues = {}
                for node in join_graph:
                    traversed_nodes_order = []
                    visited_nodes, all_adjacent_nodes = set(), set()
                    traverse_opt_plan(query, node, join_graph, visited_nodes,
                                    traversed_nodes_order, all_adjacent_nodes,
                                    distinct_sub_plans, distinct_sub_plans_trues,
                                    query_sub_plans, query_sub_plans_compass, query_sub_plans_psql)

                for system_id in range(len(output_files)):  # optimal, compass, psql plans
                    min_card, plan_size, optimal_plan = math.inf, 0, ""
                    true_cost, compass_est, psql_est = 0, 0, 0

                    # choosing the best from
                    for ii, plan in enumerate(distinct_sub_plans_trues):
                        if distinct_sub_plans_trues[plan][0] == len(join_graph) - 1:  # considering only plans before the final
                            if distinct_sub_plans_trues[plan][system_id + 1] < min_card:
                                min_card = distinct_sub_plans_trues[plan][system_id + 1]
                                true_cost = distinct_sub_plans_trues[plan][1]
                                compass_est = distinct_sub_plans_trues[plan][2]
                                psql_est = distinct_sub_plans_trues[plan][3]
                                plan_size = len(plan.split())
                                optimal_plan = plan.split()
                    info_print = [query, plan_size, true_cost, compass_est, psql_est, optimal_plan]
                    output_writers[system_id].writerow(info_print)

        [output_file_objects[system_id].close() for system_id in range(len(output_files))]
        print("Success.\n")

    except:
        print("Wrong parameter type or code error.\n")
