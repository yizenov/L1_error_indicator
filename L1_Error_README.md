# L1-Error Evaluation
We provide instructions to reproduce each figure and table in the paper.

## Table of Contents
1. [Figure 1](#figure1). Correlation of Q-error and P-error.
2. [Figure 2](#figure2). Example query 2c from JOB.
3. [Figure 3](#figure3). Query 2c cardinalities.
4. [Figure 4a](#figure4a). Evaluation of estimations via Q-error.
5. [Figure 4b](#figure4b). Subplan statistics in JOB.
6. [Figure 5a](#figure5a). Compare search algorithms.
7. [Figure 5b](#figure5b). JOB query complexity statistics.
8. [Figure 6](#figure6). Query 2c cardinality weights.
9. [Figure 7](#figure7). Join size importance.
10. [Figure 8](#figure8). Cost and runtime correlation.
11. [Figure 9](#figure9). P-error distribution.
12. [Tables 1,2,3,4](#tables1234). L1-error classifier results.

Each figure instruction specifies its figure file, Python script, input and output files. Each Python script includes the instruction on how to run along with its arguments. All scripts are run from `~/L1_error_indicator` folder.

## Figure 1. Correlation of Q-error and P-error <a name="figure1"></a>
Figure file: `figures/figure_max_qerror_perror.ods`</br>
Script file: `scripts/run_max_qerror_perror.py`</br>
Output files: `output_data/job/res_max_qerror_perror.csv`</br>
Input files:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job/exhaustive_traversals-opt-cost-psql.csv`
- `input_data/job/exhaustive_traversals-opt-cost.csv`
- `input_data/job/results_estimates-JOB.csv`

Environment description: PostgreSQL, Exhaustive Search, JOB workload.

## Figure 2. Example query 2c from JOB <a name="figure2"></a>
The figure is built based on `input_data/job/results_estimates-JOB.csv`.

## Figure 3 (a,b). Query 2c cardinalities <a name="figure3"></a>
The figure is built based on `input_data/job/results_estimates-JOB.csv`.

## Figure 4a. Evaluation of estimations via Q-error <a name="figure4a"></a>
Figure file: N/A</br>
Script file: `scripts/run_evaluate_q_error.py`</br>
Output files: `output_data/job/res_evaluate_q_error.pdf`</br>
Input files: `input_data/job/results_estimates-JOB.csv`

Environment description: PostgreSQL, JOB workload.

## Figure 4b. Subplan statistics in JOB <a name="figure4b"></a>
Figure file: `figures/figure_subplan_statistics.ods`</br>
Script file: `scripts/run_subplan_statistics.py`</br>
Output files: `output_data/job/res_subplan_statistics.csv`</br>
Input files: `input_data/job/results_estimates-JOB.csv`

Environment description: JOB workload.

## Figure 5a. Compare search algorithms <a name="figure5a"></a>
Figure file: `figures/figure_compare_enumerators.ods`</br>
Script file: `scripts/run_compare_enumerators.py`</br>
Output files: `output_data/job/res_compare_enumerators.csv`</br>
Input files:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job/exhaustive_traversals-opt-cost-psql.csv`
- `input_data/job/exhaustive_traversals-opt-cost.csv`
- `input_data/job/greedy_traversals-opt-cost-psql.csv`
- `input_data/job/greedy_traversals-opt-cost.csv`

Environment description: PostgreSQL, JOB workload.

## Figure 5b. JOB query complexity statistics <a name="figure5b"></a>
Figure file: `figures/figure_query_complexity.ods`</br>
Script file: `scripts/run_query_complexity.py`</br>
Output files: `output_data/job/res_query_complexity.csv`</br>
Input files:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job/results_estimates-JOB.csv`

Environment description: JOB workload.

## Figure 6 (a,b). Query 2c cardinality weights <a name="figure6"></a>
The figure is built based on `input_data/job/results_estimates-JOB.csv`

## Figure 7. Join size importance <a name="figure7"></a>
Figure file: `figures/figure_join_size_analysis.ods`</br>
Script file: `scripts/run_join_size_analysis.py`</br>
Output files: `output_data/job/res_join_size_analysis.csv`</br>
Input files:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job/results_estimates-JOB.csv`

Environment description: JOB workload.

## Figure 8. Cost and runtime correlation <a name="figure8"></a>
Figure file: `figures/figure_cost_runtime.ods`</br>
Script file: `scripts/run_cost_runtime.py`</br>
Output files: `output_data/job/res_cost_runtime.csv`</br>
Input files:
- `input_data/job/exhaustive_all_plans_with_costs.csv`
- `output_data/job/RESULTS_FIXED_PLAN_JOB_EXHAUSTIVE_OPT`

Environment description: PostgreSQL engine, Exhaustive Search, JOB workload - query 2c.

## Figure 9. P-error distribution <a name="figure9"></a>
Figure file: `figures/figure_perror_distribution.ods`</br>
Script file: `scripts/run_perror_distribution.py`</br>
Output files: `output_data/job/res_perror_distribution.csv`</br>
Input files:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job/greedy_traversals-opt-cost-psql.csv`
- `input_data/job/greedy_traversals-opt-cost.csv`

Environment description: PostgreSQL, Greedy Search, JOB workload.

## Table 1, 2 and 3, 4. L1-error classifier results <a name="tables1234"></a>
Figure file: N/A</br>
Script file: `scripts/run_L1_classifier.py`</br>
Output files: results are printed in the terminal</br>
Input files JOB:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job/L1-errors-agg-exhaustive-psql.csv`
- `input_data/job/L1-errors-agg-exhaustive-compass.csv`
- `input_data/job/L1-errors-agg-greedy-psql.csv`
- `input_data/job/L1-errors-agg-greedy-compass.csv`

Input files JOB-light:
- `input_data/job_light/JOB_light_QUERIES`
- `input_data/job_light/L1-light-errors-agg-exhaustive-psql.csv`
- `input_data/job_light/L1-light-errors-agg-exhaustive-compass.csv`
- `input_data/job_light/L1-light-errors-agg-greedy-psql.csv`
- `input_data/job_light/L1-light-errors-agg-greedy-compass.csv`

Environment description: PostgreSQL and COMPASS, Greedy and Exhaustive Search, JOB-light and JOB workload.
