import os
import csv
import sys

# This script is to generate all plans for Figure-8 based on Exhaustive Search, JOB workload
# Terminologies:

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts_prepare/fig8_benchmark_exhaustive_all_plans.py arg1\n \
\t Script requires 1 argument: JOB query name\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 2:
    print("Wrong number of arguments.\n")
else:
    try:
        target_query = sys.argv[1]

        workload_folder = "input_data/job/JOB_QUERIES_COMPASS_PostgreSQL"
        all_estimate_file = "input_data/job/results_estimates-JOB.csv"
        output_f_file = "input_data/job/exhaustive_all_plans_with_costs.csv"

        output_f = open(output_f_file, "w")
        output_f_writer = csv.writer(output_f, delimiter=',')
        output_f_writer.writerow(["query name", "plan size", "true cost", "plan"])

        ####################### Prepare Cardinalities #######################################

        query_sub_plans, query_sub_plans_psql, query_sub_plans_compass = {}, {}, {}
        missing_subplans = {}
        with open(all_estimate_file, "r") as query_input_f:
            for idx, line in enumerate(query_input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                query = line[0].split("_")[0]
                if query not in query_sub_plans: query_sub_plans[query] = {}
                if query not in query_sub_plans_compass: query_sub_plans_compass[query] = {}
                if query not in query_sub_plans_psql: query_sub_plans_psql[query] = {}

                plan_size = int(line[1])
                true_cardinality = float(line[2])
                psql_estimate = float(line[3])
                compass_estimate = float(line[4])

                if true_cardinality < 0:
                    if query not in missing_subplans: missing_subplans[query] = 0
                    missing_subplans[query] += 1
                if true_cardinality == 0: print("zero true cardinality: " + query)
                if psql_estimate < 0: print("psql estimate is negative: " + query)
                if psql_estimate == 0: print("zero psql estimate cardinality: " + query)
                if compass_estimate < 0: print("compass estimate is negative: " + query)
                if compass_estimate == 0: print("zero compass_estimate cardinality: " + query)

                plan = ",".join(line[5:])[2:-2]
                plan = plan.split(",")
                plan = sorted([node.strip()[1:-1].split("-")[1] for node in plan])

                plan = [" " + node + " " for node in plan]
                plan = " ".join(plan)
                if plan not in query_sub_plans[query]:
                    query_sub_plans[query][plan] = true_cardinality
                    query_sub_plans_compass[query][plan] = compass_estimate
                    query_sub_plans_psql[query][plan] = psql_estimate
        print("\nmissing subplans:")
        for query in missing_subplans:
            print("\t" + query + ": " + str(missing_subplans[query]))

        ####################### Exhaustive Traversal ################################################

        def traverse_opt_plan(param_node, param_join_graph, param_visited_nodes,
                            param_traversed_nodes_order, param_all_adjacent_nodes,
                            param_distinct_sub_plans, param_distinct_sub_plans_trues,
                            param_all_sub_plans, param_all_sub_plans_compass, 
                            param_all_sub_plans_psql, query, set_count_plan, count_plan):
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
                    # NOTE: can be omitted, it is only printing current stage

                if len(param_traversed_nodes_order) == len(param_join_graph):
                    temp_plan = [n.strip() for n in param_traversed_nodes_order]
                    temp_true_cost = param_distinct_sub_plans_trues[plan_string][1]
                    count_plan[0] += 1
                    set_count_plan.add(" ".join(temp_plan))
                    output_f_writer.writerow([query + "_" + str(count_plan[0]), 
                            len(param_traversed_nodes_order), temp_true_cost, temp_plan])
                    return

            adj_nodes = [adj for adj in param_join_graph[param_node] if adj not in param_visited_nodes]
            [param_all_adjacent_nodes.add(adj) for adj in adj_nodes]

            for adj in adj_nodes:
                if adj not in param_visited_nodes:
                    local_adj_nodes = set(temp_adj for temp_adj in param_all_adjacent_nodes if temp_adj not in param_visited_nodes and temp_adj != adj)
                    traverse_opt_plan(adj, param_join_graph, param_visited_nodes,
                                    param_traversed_nodes_order, local_adj_nodes,
                                    param_distinct_sub_plans, param_distinct_sub_plans_trues,
                                    param_all_sub_plans, param_all_sub_plans_compass, param_all_sub_plans_psql, 
                                    query, set_count_plan, count_plan)
                    param_visited_nodes.remove(adj)
                    param_traversed_nodes_order.pop()

            open_adj_nodes = [adj for adj in param_all_adjacent_nodes if adj not in param_visited_nodes and adj not in adj_nodes]
            [param_all_adjacent_nodes.add(adj) for adj in open_adj_nodes]

            for adj in open_adj_nodes:
                if adj not in param_visited_nodes:
                    local_adj_nodes = set(temp_adj for temp_adj in param_all_adjacent_nodes if temp_adj not in param_visited_nodes and temp_adj != adj)
                    traverse_opt_plan(adj, param_join_graph, param_visited_nodes,
                                    param_traversed_nodes_order, local_adj_nodes,
                                    param_distinct_sub_plans, param_distinct_sub_plans_trues,
                                    param_all_sub_plans, param_all_sub_plans_compass, param_all_sub_plans_psql, 
                                    query, set_count_plan, count_plan)
                    param_visited_nodes.remove(adj)
                    param_traversed_nodes_order.pop()

        ############################################################################################

        total_nbr_plans, set_count_plan, count_plan = 0, set(), [0]
        print("\nExtracting distinct sub and full plans from a join graph:")
        for f_idx, file_name in enumerate(sorted(os.listdir(workload_folder))):
            query = file_name.split(".")[0].split("_")[1]
            if query != target_query: continue

            join_graph, distinct_sub_plans = {}, set()

            input_query = workload_folder + "/" + file_name
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

                print("\tstarting-----" + query)
                # key[plan] = [join_size, true_card, compass_est, psql_est, all]
                distinct_sub_plans_trues = {}
                for node in join_graph:
                    traversed_nodes_order = []
                    visited_nodes, all_adjacent_nodes = set(), set()
                    traverse_opt_plan(node, join_graph, visited_nodes,
                                    traversed_nodes_order, all_adjacent_nodes,
                                    distinct_sub_plans, distinct_sub_plans_trues,
                                    query_sub_plans, query_sub_plans_compass, query_sub_plans_psql, 
                                    query, set_count_plan, count_plan)
                total_nbr_plans += len(distinct_sub_plans)

        print("\nSub-plans count: " + str(total_nbr_plans))
        print("Count plan: " + str(count_plan))
        print("Set count plan: " + str(len(set_count_plan)) + "\n")

        output_f.close()
        print("The results will be saved at: " + output_f_file)
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
