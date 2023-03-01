import os
import sys
import csv
import math

ZERO_REPLACE = 0.0001  # in case denominators are equal to zero

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts/L1_error_job_light_exhaustive.py arg1\n \
\t Script requires 1 arguments\n \
\t Optimizer: (1 = COMPASS, 0 = PostgreSQL)\n \
")

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

def similarity_weights_L1_error_combined(true_subs, est_subs, true_card):
    # similarity weights: D_i >= 0 and diagonals = 1
    # position weights: delta d_i >= 0 and rho p_1 = 1
    n = len(true_subs)

    mu_weights = [1]  # monotonic increase
    [mu_weights.append(mu_weights[-1] + true_card[true_subs[i]] / max(true_card[true_subs[i - 1]], ZERO_REPLACE)) for i in range(1, n)]

    # NOTE: similar to Q-error
    D_weights = {}
    for sub in true_subs:
        D_weights[sub] = {}
        for inner_sub in true_subs:
            # NOTE: in case of zero cardinality, use ZERO_REPLACE value
            q_error = max([true_card[inner_sub] / max(true_card[sub], ZERO_REPLACE), 
                        true_card[sub] / max(true_card[inner_sub], ZERO_REPLACE)])
            D_weights[sub][inner_sub] = round(q_error, 2)

    # NOTE: symmetric part
    loc_cost = 0
    for t_id, sub in enumerate(true_subs):
        est_id = est_subs.index(sub)
        cost_2, cost_3 = 0, 0
        for i in range(t_id):
            e_id = est_subs.index(true_subs[i])
            if e_id > est_id:
                cost_2 += D_weights[sub][true_subs[i]]
        for i in range(t_id + 1, n):
            e_id = est_subs.index(true_subs[i])
            if e_id < est_id:
                cost_3 += D_weights[sub][true_subs[i]] * (mu_weights[i] - mu_weights[e_id])
        loc_cost += cost_2 + cost_3
    diff_sum = round(loc_cost, 2)
    return [diff_sum, diff_sum, n]

if len(sys.argv) != 2:
    print("Wrong number of arguments.\n")
else:
    try:
        is_compass = int(sys.argv[1])

        PERFORMANCE_RATIO = 1.0
        JOINS_TO_CONSIDER = 5
        LOGISTIC_GROWTH_RATE = -1.5

        if is_compass not in [0, 1]: 
            print("Wrong argument types.\n")
        else:
            query_files = "input_data/job_light/JOB_light_QUERIES"

            true_cost_data_file = "input_data/job_light/exhaustive_traversals-opt-cost.csv"
            if is_compass:
                cardinality_file, est_index = "input_data/job_light/results_estimates-JOB-light.csv", 4
                plans_file = "input_data/job_light/exhaustive_traversals-opt-cost-compass.csv"
                output_f_file = "input_data/job_light/L1-light-errors-agg-exhaustive-compass.csv"
            else:
                cardinality_file, est_index = "input_data/job_light/results_estimates-JOB-light.csv", 3
                plans_file = "input_data/job_light/exhaustive_traversals-opt-cost-psql.csv"
                output_f_file = "input_data/job_light/L1-light-errors-agg-exhaustive-psql.csv"

            plans_opt_costs = {}
            with open(true_cost_data_file, "r") as input_f:
                for idx, line in enumerate(input_f):
                    if idx == 0: continue
                    line = line.split(",")
                    plans_opt_costs[line[0]] = float(line[2])

            all_plans = {}
            with open(plans_file, "r") as input_f:  # only left-deep plans
                for idx, line in enumerate(input_f):

                    if idx == 0: continue
                    line = line.strip().split(",")
                    query, true_cost = line[0], float(line[2])

                    plan = ",".join(line[5:])[2:-2].split(",")
                    plan = [node.strip()[1:-1] for node in plan]

                    all_plans[query] = [plan, true_cost]

            all_true_cardinalities, all_queries = {}, {}
            with open(cardinality_file, "r") as input_f:
                for idx, line in enumerate(input_f):

                    if idx == 0: continue
                    line = line.split(",")

                    query_family = line[0].split("_")[0]
                    subquery, true_card = line[0], float(line[2].strip())

                    if query_family not in all_true_cardinalities: all_true_cardinalities[query_family] = {}
                    all_true_cardinalities[query_family][subquery] = [true_card]

                    if query_family not in all_queries: all_queries[query_family] = {}
                    all_queries[query_family][subquery] = int(line[1])

            all_estimated_cardinalities, subquery_plans = {}, {}
            with open(cardinality_file, "r") as input_f:
                for idx, line in enumerate(input_f):

                    if idx == 0: continue
                    line = line.strip().split(",")

                    query_family = line[0].split("_")[0]
                    subquery, est_card = line[0], float(line[est_index].strip())

                    if query_family not in all_estimated_cardinalities: all_estimated_cardinalities[query_family] = {}
                    all_estimated_cardinalities[query_family][subquery] = [est_card]

                    if query_family not in subquery_plans: subquery_plans[query_family] = {}
                    subplan = ",".join(line[5:])[2:-2].split(",")
                    subplan = [node.strip()[1:-1].split("-")[1] for node in subplan]
                    subquery_plans[query_family][subquery] = subplan

            ##################### Update card with card + cost ##########################################

            for query in subquery_plans:
                if int(query) in [1, 2, 3]: continue  # NOTE: two-way joins only
                
                max_join = 0
                for subquery in subquery_plans[query]:
                    join_size = len(subquery_plans[query][subquery])
                    max_join = max(max_join, join_size)
                    if join_size == 2:
                        est_card = all_estimated_cardinalities[query][subquery][0]
                        all_estimated_cardinalities[query][subquery].append(est_card)
                        true_card = all_true_cardinalities[query][subquery][0]
                        all_true_cardinalities[query][subquery].append(true_card)

                for join in range(3, max_join):
                    for subquery in subquery_plans[query]:
                        join_size = len(subquery_plans[query][subquery])
                        if join_size == join:
                            min_est_cost, min_true_cost = float('inf'), float('inf')
                            for loc_subquery in subquery_plans[query]:
                                is_subplan = True
                                for nick in subquery_plans[query][loc_subquery]:
                                    if nick not in subquery_plans[query][subquery]:
                                        is_subplan = False
                                        break
                                
                                loc_join_size = len(subquery_plans[query][loc_subquery])
                                if is_subplan and (join_size - 1) == loc_join_size:
                                    est_cost = all_estimated_cardinalities[query][loc_subquery][1]
                                    if est_cost < min_est_cost:
                                        min_est_cost = est_cost
                                    true_cost = all_true_cardinalities[query][loc_subquery][1]
                                    if true_cost < min_true_cost:
                                        min_true_cost = true_cost
                            new_est_cost = all_estimated_cardinalities[query][subquery][0] + min_est_cost
                            all_estimated_cardinalities[query][subquery].append(new_est_cost)
                            new_true_cost = all_true_cardinalities[query][subquery][0] + min_true_cost
                            all_true_cardinalities[query][subquery].append(new_true_cost)
                
            #############################################################################################

            input_query_files = sorted(os.listdir(query_files))
            query_meta_info, l1_distances = {}, []
            for idx, file_name in enumerate(input_query_files):
                query_name = file_name[:-4]
                if int(query_name) in [1, 2, 3]: continue  # NOTE: two-way joins only
                query_plan = all_plans[query_name][0]

                input_query = query_files + "/" + file_name
                with open(input_query, "r") as query_input_f:

                    original_query = [query_line for query_line in query_input_f]
                    original_query = "".join(original_query)

                    # extracting tables and join predicates
                    from_and_where = original_query.split('FROM')[1].split('WHERE')
                    table_list = from_and_where[0].split(',')
                    table_list = [table.strip() for table in table_list]

                    # collecting tables and join predicates information
                    where_clause = from_and_where[1].split('\n\n')
                    where_clause = [clause_set for clause_set in where_clause if clause_set]
                    join_predicates = where_clause[1].split('AND')
                    join_predicates = [join.strip() for join in join_predicates if join.strip()]
                    join_predicates[-1] = join_predicates[-1][:-1]
                    if query_name not in query_meta_info: query_meta_info[query_name] = [0, 0]
                    query_meta_info[query_name][0] = max(len(table_list), query_meta_info[query_name][0])
                    query_meta_info[query_name][1] = max(len(join_predicates), query_meta_info[query_name][1])

                    # NOTE: add +1 if last join is included
                    for join_size in range(2, len(table_list)):
                        true_cards, est_cards = {}, {}
                        for sub in all_queries[query_name]:
                            if all_queries[query_name][sub] == join_size:
                                true_cards[sub] = all_true_cardinalities[query_name][sub][1]
                                est_cards[sub] = all_estimated_cardinalities[query_name][sub][1]

                        ordered_true_cards = {k: v for k, v in sorted(true_cards.items(), key=lambda item: item[1])}
                        ordered_true_subs = [sub for sub in ordered_true_cards]
                        ordered_est_cards = {k: v for k, v in sorted(est_cards.items(), key=lambda item: item[1])}
                        ordered_est_subs = [sub for sub in ordered_est_cards]

                        q_error_values, max_q_error = [], -1
                        for i in range(len(ordered_est_subs)):
                            sub = ordered_true_subs[i]
                            t_card = ordered_true_cards[sub]
                            e_card = ordered_est_cards[sub]
                            q_error_curr = round(max([e_card / max(t_card, ZERO_REPLACE), t_card / max(e_card, ZERO_REPLACE)]), 2)
                            max_q_error = max(max_q_error, q_error_curr)
                            q_error_values.append(q_error_curr)

                        delta_weights, weights = [1.0], [1.0]
                        for i in range(1, len(ordered_true_subs)):
                            sub_curr = ordered_true_subs[i]
                            sub_prev = ordered_true_subs[i - 1]
                            t_card_curr = ordered_true_cards[sub_curr]
                            t_card_prev = ordered_true_cards[sub_prev]
                            w_curr = round(t_card_curr / max(t_card_prev, ZERO_REPLACE), 2)
                            delta_weights.append(w_curr)
                            weights.append(round(weights[i - 1] + w_curr, 2))

                        z_weights = []
                        for true_i in range(len(ordered_true_subs)):
                            sub = ordered_true_subs[true_i]
                            est_i = ordered_est_subs.index(sub)
                            z = abs((weights[true_i] - weights[est_i]) / max(1, (true_i + 1 - est_i - 1)))
                            z_weights.append(round(z, 2))

                        results = similarity_weights_L1_error_combined(ordered_true_subs, ordered_est_subs, ordered_true_cards)

                        l1_info = [query_name, join_size, results[0], results[1], results[2], len(ordered_true_subs), max_q_error]
                        l1_distances.append(l1_info)
        
            ############################# Aggregation all sub-queries with the same join size ##############################

            comp_counter = [set(), set(), set()]
            optimal_c, sub_optimal_c = set(), set()
            better_than_optimal, max_join = set(), 0

            for c_idx, complex in enumerate([[1, 4]]):
                for l1 in l1_distances:
                    query_complex = query_meta_info[l1[0]][1]
                    ratio = round(all_plans[l1[0]][1] / plans_opt_costs[l1[0]], 2)
                    if ratio < 1.0: better_than_optimal.add(l1[0])
                    if complex[0] <= query_complex <= complex[1] and l1[1] < JOINS_TO_CONSIDER and ratio > PERFORMANCE_RATIO:
                        sub_optimal_c.add(l1[0])
                        comp_counter[c_idx].add(l1[0])
                        max_join = max(max_join, l1[1])
                for l1 in l1_distances:
                    query_complex = query_meta_info[l1[0]][1]
                    ratio = round(all_plans[l1[0]][1] / plans_opt_costs[l1[0]], 2)
                    if ratio < 1.0: better_than_optimal.add(l1[0])
                    if complex[0] <= query_complex <= complex[1] and l1[1] < JOINS_TO_CONSIDER  and ratio <= PERFORMANCE_RATIO:
                        optimal_c.add(l1[0])
                        comp_counter[c_idx].add(l1[0])
                        max_join = max(max_join, l1[1])

            print("\nmax join: " + str(max_join))
            print("simple, moderate, complex, TOTAL: " + 
                str([len(comp_counter[0]), len(comp_counter[1]), len(comp_counter[2]),
                    len(comp_counter[0]) + len(comp_counter[1]) + len(comp_counter[2])
                ]))
            print("optimal, sub-optimal, TOTAL: " + str([len(optimal_c), len(sub_optimal_c),
                len(optimal_c) + len(sub_optimal_c)
            ]))
            print("better selected plans than true exhaustive optimal: " + str(len(better_than_optimal)))

            ############################# Aggregation across join sizes ###########################################

            output_f = open(output_f_file, "w")
            output_f_writer = csv.writer(output_f, delimiter=',')
            output_f_writer.writerow(["query", "tables", 
                "predicates", "cost ratio", "label", 
                "MAX Q-error", "SUM Q-error", 
                "MAX L1-error", "SUM L1-error", "Weighted SUM L1-error", "query complexity"])

            for c_idx, complex in enumerate([[1, 4]]):
                for query in all_queries:
                    if query not in query_meta_info: continue
                    label, query_complex = 1, query_meta_info[query][1]
                    cost_ratio = round(all_plans[query][1] / plans_opt_costs[query], 2)
                    if complex[0] <= query_complex <= complex[1] and cost_ratio > PERFORMANCE_RATIO: 
                        max_norm_L1, sum_norm_L1, weighted_sum_l1 = -1, 0, 0
                        max_q_error, sum_q_error = -1, 0
                        for l1 in l1_distances:
                            if l1[0] == query and l1[1] < JOINS_TO_CONSIDER:
                                join_weight = math.exp(LOGISTIC_GROWTH_RATE * l1[1]) / (1 + math.exp(LOGISTIC_GROWTH_RATE * l1[1]))
                                max_norm_L1 = max(max_norm_L1, l1[2])
                                sum_norm_L1 += l1[2]
                                weighted_sum_l1 += l1[2] * join_weight
                                max_q_error = max(max_q_error, l1[6])
                                sum_q_error += l1[6]
                        output_f_writer.writerow([query, 
                            query_meta_info[query][0], 
                            query_complex, cost_ratio, label, 
                            max_q_error, sum_q_error, 
                            max_norm_L1, sum_norm_L1, weighted_sum_l1, c_idx + 1])
                output_f_writer.writerow(["*","*","*","*","*","*","*","*","*","*","*"])
                for query in all_queries:
                    if query not in query_meta_info: continue
                    label, query_complex = 0, query_meta_info[query][1]
                    cost_ratio = round(all_plans[query][1] / plans_opt_costs[query], 2)
                    if complex[0] <= query_complex <= complex[1] and cost_ratio <= PERFORMANCE_RATIO: 
                        max_norm_L1, sum_norm_L1, weighted_sum_l1 = -1, 0, 0
                        max_q_error, sum_q_error = -1, 0
                        for l1 in l1_distances:
                            if l1[0] == query and l1[1] < JOINS_TO_CONSIDER:
                                join_weight = math.exp(LOGISTIC_GROWTH_RATE * l1[1]) / (1 + math.exp(LOGISTIC_GROWTH_RATE * l1[1]))
                                max_norm_L1 = max(max_norm_L1, l1[2])
                                sum_norm_L1 += l1[2]
                                weighted_sum_l1 += l1[2] * join_weight
                                max_q_error = max(max_q_error, l1[6])
                                sum_q_error += l1[6]
                        output_f_writer.writerow([query, 
                            query_meta_info[query][0], 
                            query_complex, cost_ratio, label, 
                            max_q_error, sum_q_error, 
                            max_norm_L1, sum_norm_L1, weighted_sum_l1, c_idx + 1])
                output_f_writer.writerow(["#","#","#","#","#","#","#","#","#","#","#"])
                output_f_writer.writerow(["#","#","#","#","#","#","#","#","#","#","#"])

        output_f.close()
        print("\nThe results will be saved at: " + output_f_file)
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
