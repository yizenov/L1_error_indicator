import os
import csv
import math
import sys

# This script is to prepare greedy plans on JOB and JOB-light workload
# Terminologies: optimal (opt), PostgreSQL (psql), COMPASS (compass)

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts_prepare/collect_greedy_plans.py arg1 arg2\n \
\t Script requires 2 argument\n \
\t Workload: (1 = JOB-light, 0 = JOB)\n \
\t Cardinality: (0 = True, 1 = PostgreSQL, 2 = COMPASS)\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 3:
    print("Wrong number of arguments.\n")
else:
    try:
        is_job_light = int(sys.argv[1])
        engine_idx = int(sys.argv[2])

        workload_folder = "input_data/job/JOB_QUERIES_COMPASS_PostgreSQL"
        if is_job_light: workload_folder = "input_data/job_light/JOB_light_QUERIES"

        all_estimate_file = "input_data/job/results_estimates-JOB.csv"
        if is_job_light: all_estimate_file = "input_data/job_light/results_estimates-JOB-light.csv"

        parent_folder = "input_data/job/"
        if is_job_light: parent_folder = "input_data/job_light/"

        if engine_idx == 0: output_f_file_opt = parent_folder + "greedy_traversals-opt-cost.csv"
        elif engine_idx == 1: output_f_file_opt = parent_folder + "greedy_traversals-opt-cost-psql.csv"
        elif engine_idx == 2: output_f_file_opt = parent_folder + "greedy_traversals-opt-cost-compass.csv"

        output_f_opt = open(output_f_file_opt, "w")
        output_f_writer_opt = csv.writer(output_f_opt, delimiter=',')
        output_f_writer_opt.writerow(["query_name", "plan_size", "true_cost", "compass_est_cost", "psql_est_cost", "plan"])

        ######################################################################################

        query_sub_plans = {}
        with open(all_estimate_file, "r") as query_input_f:
            for idx, line in enumerate(query_input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                query = line[0].split("_")[0]
                if query not in query_sub_plans: query_sub_plans[query] = {}

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
                    query_sub_plans[query][plan] = [true_cardinality, psql_est_cardinality, compass_est_cardinality]

        ######################################################################################

        def traverse_opt_plan(query_sub_plans, max_plan_size, engine_idx):
            true_cost, psql_est_cost, compass_est_cost, prev_sub_plan, steps = 0, 0, 0, "", []
            for sub_size in range(2, max_plan_size):
                min_cost, min_true_cost, min_psql_est_cost, min_compass_est_cost = math.inf, math.inf, math.inf, math.inf
                temp_sub_plan = ""
                all_sub_plans = {plan:query_sub_plans[plan] for plan in query_sub_plans if len(plan.split()) == sub_size}

                for plan in all_sub_plans:
                    if sub_size == 2:
                        if min_cost > query_sub_plans[plan][engine_idx]:
                            min_cost = query_sub_plans[plan][engine_idx]

                            min_true_cost = query_sub_plans[plan][0]
                            min_psql_est_cost = query_sub_plans[plan][1]
                            min_compass_est_cost = query_sub_plans[plan][2]
                            temp_sub_plan = plan
                    else:
                        found = True
                        for rel in prev_sub_plan.split():
                            rel = rel.strip()
                            all_tables = [tab.strip() for tab in plan.split()]
                            if rel not in all_tables:
                                found = False
                                break
                        if found and min_cost > query_sub_plans[plan][engine_idx]:
                            min_cost = query_sub_plans[plan][engine_idx]

                            min_true_cost = query_sub_plans[plan][0]
                            min_psql_est_cost = query_sub_plans[plan][1]
                            min_compass_est_cost = query_sub_plans[plan][2]
                            temp_sub_plan = plan
                true_cost += min_true_cost
                psql_est_cost += min_psql_est_cost
                compass_est_cost += min_compass_est_cost
                prev_sub_plan = temp_sub_plan
                steps.append(prev_sub_plan.split())

            return max_plan_size - 1, true_cost, psql_est_cost, compass_est_cost, prev_sub_plan, steps

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

                pl_size, true_cost, psql_est_cost, compass_est_cost, pl_plan, steps = traverse_opt_plan(query_sub_plans[query], len(join_graph), engine_idx)
                plan_final = []
                for step in steps:
                    for rel in step:
                        if rel not in plan_final:
                            plan_final.append(rel)

                info_print = [query, pl_size, true_cost, compass_est_cost, psql_est_cost, plan_final]
                output_f_writer_opt.writerow(info_print)

        output_f_opt.close()
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
