import sys
import os
import pandas as pd

# CART (Classification and Regression Trees)
    # scikit-learn uses an optimised version of the CART algorithm.
    # https://scikit-learn.org/stable/modules/tree.html#tree-algorithms-id3-c4-5-c5-0-and-cart
# C4.5, C5.0, ID3
from sklearn.tree import DecisionTreeClassifier

# from sklearn.grid_search import GridSearchCV
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split

from sklearn.metrics import confusion_matrix
from sklearn.metrics import make_scorer, accuracy_score
from sklearn.metrics import precision_score, recall_score, f1_score

# This script is to build Table 1, 2, 3, 4 
#   running Greedy and Exhaustive Searchs
#   based on JOB-light and JOB workloads
# Terminologies: PostgreSQL, COMPASS

print(
"\n \
1. Enter: ~/L1_error_indicator\n \
2. Run the following command: /usr/bin/python3 scripts/run_L1_classifier.py arg1 arg2 arg3\n \
\t Script requires 3 arguments:\n \
\t a) Workload: (1 = JOB-light, 0 = JOB)\n \
\t b) Optimizer: (1 = COMPASS, 0 = PostgreSQL)\n \
\t c) Search algorithm: (1 = Greedy, 0 = Exhaustive)\n \
")

###################### Output File Name #########################

print('Number of arguments:', len(sys.argv) - 1, 'arguments.')
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 4:
    print("Wrong number of arguments.\n")
else:
    try:
        is_job_light = int(sys.argv[1])
        is_compass = int(sys.argv[2])
        is_greedy = int(sys.argv[3])

        iinput_queries = "input_data/job/JOB_QUERIES_COMPASS_PostgreSQL/"
        file_name_prefixes = "input_data/job/L1-errors-agg-"
        if is_job_light:
            input_queries = "input_data/job_light/JOB_light_QUERIES/"
            file_name_prefixes = "input_data/job_light/L1-light-errors-agg-"

        if not is_greedy: file_name_prefixes += "exhaustive-"
        else: file_name_prefixes += "greedy-"

        if is_compass: file_name_prefixes += "compass.csv"
        else: file_name_prefixes += "psql.csv"

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
                if is_job_light: query = file_name.split(".")[0]
                query_complexities[query] = len(join_predicates)

        ###################### Data Preparation #########################

        initial_all_data, query_costs = [], {}
        true_negatives, true_positives = 0, 0
        with open(file_name_prefixes, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0 or "*" in line or "#" in line: continue
                line = line.strip().split(',')

                query, cost = line[0], round(float(line[3]), 2)
                sum_norm_L1, label = round(float(line[8]), 2), int(line[4])

                if label == 0: true_negatives += 1
                else: true_positives += 1

                if query not in query_costs: query_costs[query] = cost
                initial_all_data.append([query, sum_norm_L1, label]) # NOTE: feature data

        pd.set_option('display.max_rows', 500)
        column_names = ["query", "sum weighted L1", "class"]
        data_df = pd.DataFrame(initial_all_data, columns = column_names)

        ###################### Classifier Configuration #########################

        # test and train size split
        TEST_SIZE = 0.4

        # https://scikit-learn.org/stable/modules/generated/sklearn.tree.DecisionTreeClassifier.html#sklearn.tree.DecisionTreeClassifier
        RANDOME_STATE = 42
        MAX_DEPTH = 1
        splitter_type = "best"  # best (def), random
        criteria_name = "entropy" # gini (def), entropy, log_loss
        enable_refit = 'accuracy'
        scoring_type = {
            "accuracy": make_scorer(accuracy_score),
            "precision": make_scorer(precision_score),
            "recall": make_scorer(recall_score),
            "f1_score": make_scorer(f1_score)
        }
        
        # grid search to find better split threshold
        # https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.GridSearchCV.html
        cross_validation_size = 7
        MIN_SAMPLES_SPLIT = 20
        MIN_SAMPLES_LEAF = 10
        if is_job_light:
            cross_validation_size = 3
            MIN_SAMPLES_SPLIT = 3
            MIN_SAMPLES_LEAF = 2

        tree_params = {
            "criterion": ['gini', 'entropy', 'log_loss'],
            "max_depth": [MAX_DEPTH],
            "min_samples_split": range(2, MIN_SAMPLES_SPLIT),
            "min_samples_leaf": range(1, MIN_SAMPLES_LEAF),
            "splitter": ["best"],
            "random_state": [RANDOME_STATE]
        }

        ###################### Classifier #########################

        dt_clf = DecisionTreeClassifier(criterion=criteria_name,
                splitter=splitter_type,
                random_state=RANDOME_STATE, 
                max_depth=MAX_DEPTH, 
                min_samples_split=MIN_SAMPLES_SPLIT,
                min_samples_leaf=MIN_SAMPLES_LEAF)
        dt_clf = GridSearchCV(dt_clf, 
            param_grid=tree_params, 
            cv=cross_validation_size,
            refit=enable_refit,
            scoring=scoring_type)

        feature_indexes = [i for i in range(1, len(initial_all_data[0]) - 1)]
        label_index = len(initial_all_data[0]) - 1
        all_data = pd.DataFrame(data_df.iloc[:, feature_indexes])
        all_label_data = pd.DataFrame(data_df.iloc[:, label_index])

        X_train, X_test, y_train, y_test = train_test_split(all_data, all_label_data, 
            test_size=TEST_SIZE, 
            shuffle=True,
            random_state=RANDOME_STATE)

        dt_clf.fit(X_train, y_train)
        y_predict_train = dt_clf.predict(X_train)
        y_predict_test = dt_clf.predict(X_test)

        print("-------------")
        print("Confusion Matrix: \n\tTN, FP\n\tFN, TP")
        # TN - 0's or true fast/good queries
        # TP - 1's or true slow/bad queries
        tn_train, fp_train, fn_train, tp_train = confusion_matrix(y_train, y_predict_train).ravel()
        print("train: ")
        print(confusion_matrix(y_train, y_predict_train))
        tn_test, fp_test, fn_test, tp_test = confusion_matrix(y_test, y_predict_test).ravel()
        print("test:  \n" + str(confusion_matrix(y_test, y_predict_test)))

        tn, fp, fn, tp = tn_test + tn_train, fp_test + fp_train, fn_test + fn_train, tp_test + tp_train
        print("\n          estimated bad     |estimated good ")
        print("true bad |      " + str(tp) + " (TP)     |       " + str(fn) + " (FN)")
        print("true good|      " + str(fp) + " (FP)     |       " + str(tn) + " (TN)")

        print("-------------")
        print("Total: " + str(true_positives + true_negatives))
        print("\tsize of train data and label: " + str(len(X_train)) + ", " + str(len(y_train)))
        print("\tsize of test data and label: " + str(len(X_test)) + ", " + str(len(y_test)))
        print("Total True Positives: " + str(true_positives))  # equal to (fn + tp)
        print("Total True Negatives: " + str(true_negatives))  # equal to (fp + tn)
        print("\nTotal Estimated Positives: " + str(fp + tp))
        print("Total Estimated Negatives: " + str(fn + tn) + "\n")
        score_train = round(accuracy_score(y_train, y_predict_train), 2)
        print("Prediction Accuracy (Train): " + str(score_train))
        score_test = round(accuracy_score(y_test, y_predict_test), 2)
        print("Prediction Accuracy (Test): " + str(score_test))
        print("-------------")

        false_n, false_p = 0, 0
        fn_query_costs, fp_query_costs = {}, {}
        for idx, query_idx in enumerate(y_train.index):
            query, label = initial_all_data[query_idx][0], initial_all_data[query_idx][2]

            max_join, level = query_complexities[query], -1
            if max_join < 10: level = "Simple"
            elif 9 < max_join < 20: level = "Moderate"
            else: level = "Complex"

            if label == 1 and label != y_predict_train[idx]:
                false_n += 1
                fn_query_costs[query] = [query, query_costs[query], level]
            elif label == 0 and label != y_predict_train[idx]:
                false_p += 1
                fp_query_costs[query] = [query, query_costs[query], level]
        print("train data stats (FN, FP):" + str([false_n, false_p]))
        print("\nFN train queries:")
        [print(fn_query_costs[query]) for query in fn_query_costs]
        print("\nFP train queries:")
        [print(fp_query_costs[query]) for query in fp_query_costs]
        print("-------------")

        false_n, false_p = 0, 0
        fn_query_costs, fp_query_costs = {}, {}
        for idx, query_idx in enumerate(y_test.index):
            query, label = initial_all_data[query_idx][0], initial_all_data[query_idx][2]

            max_join, level = query_complexities[query], -1
            if max_join < 10: level = "Simple"
            elif 9 < max_join < 20: level = "Moderate"
            else: level = "Complex"

            if label == 1 and label != y_predict_test[idx]:
                false_n += 1
                fn_query_costs[query] = [query, query_costs[query], level]
            elif label == 0 and label != y_predict_test[idx]:
                false_p += 1
                fp_query_costs[query] = [query, query_costs[query], level]
        print("test data stats (FN, FP):" + str([false_n, false_p]))
        print("\nFN test queries:")
        [print(fn_query_costs[query]) for query in fn_query_costs]
        print("\nFP test queries:")
        [print(fp_query_costs[query]) for query in fp_query_costs]

        print("-------------")
        print("\nThe results will be only printed")
        print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")