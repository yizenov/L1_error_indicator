# Additional scripts to prepare all input files needed for L1-error results

- These figures are available
    * `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
    * `input_data/job_light/JOB_light_QUERIES`
- These figures are available
    * `input_data/job/results_estimates-JOB.csv`
    * `input_data/job_light/results_estimates-JOB-light.csv`
- [Part 1](all_opt_exhaustive_plans). Generate all optimal plans via Exhaustive Search
    * `input_data/job/exhaustive_traversals-opt-cost.csv`
    * `input_data/job/exhaustive_traversals-opt-cost-psql.csv`
    * `input_data/job/exhaustive_traversals-opt-cost-compass.csv`
    * `input_data/job_light/exhaustive_traversals-opt-cost.csv`
    * `input_data/job_light/exhaustive_traversals-opt-cost-psql.csv`
    * `input_data/job_light/exhaustive_traversals-opt-cost-compass.csv`
- [Part 2](all_opt_greedy_plans). Generate all optimal plans via Greedy Search
    * `input_data/job/greedy_traversals-opt-cost.csv`
    * `input_data/job/greedy_traversals-opt-cost-psql.csv`
    * `input_data/job/greedy_traversals-opt-cost-compass.csv`
    * `input_data/job_light/greedy_traversals-opt-cost.csv`
    * `input_data/job_light/greedy_traversals-opt-cost-psql.csv`
    * `input_data/job_light/greedy_traversals-opt-cost-compass.csv`
- These input files are generated for [Figure 8](figures/figure_cost_runtime.ods) 
    * [Part 3](all_query_plans). Generate all plans per query. `input_data/job/exhaustive_all_plans_with_costs.csv`
    * [Part 4](sql_files). Generate all sql query files per query. `input_data/job/RESULTS_FIXED_PLAN_JOB_EXHAUSTIVE_OPT`
- [Part 5](#l1_features). These input files are created by "L1-error features"
    * `input_data/job/L1-errors-agg-exhaustive-psql.csv`
    * `input_data/job/L1-errors-agg-exhaustive-compass.csv`
    * `input_data/job/L1-errors-agg-greedy-psql.csv`
    * `input_data/job/L1-errors-agg-greedy-compass.csv`
    * `input_data/job_light/L1-light-errors-agg-exhaustive-psql.csv`
    * `input_data/job_light/L1-light-errors-agg-exhaustive-compass.csv`
    * `input_data/job_light/L1-light-errors-agg-greedy-psql.csv`
    * `input_data/job_light/L1-light-errors-agg-greedy-compass.csv`

## 1. Generate all optimal plans via Exhaustive Search <a name="all_opt_exhaustive_plans"></a>
Figure file: N/A</br>
Script file: `scripts_prepare/collect_exhaustive_plans.py`

Output file JOB-light and JOB:
- `input_data/job_light/exhaustive_traversals-opt-cost.csv`
- `input_data/job_light/exhaustive_traversals-opt-cost-psql.csv`
- `input_data/job_light/exhaustive_traversals-opt-cost-compass.csv`
- `input_data/job/exhaustive_traversals-opt-cost.csv`
- `input_data/job/exhaustive_traversals-opt-cost-psql.csv`
- `input_data/job/exhaustive_traversals-opt-cost-compass.csv`

Input files JOB-light and JOB:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job_light/JOB_light_QUERIES`
- `input_data/job/results_estimates-JOB.csv`
- `input_data/job_light/results_estimates-JOB-light.csv`

## 2. Generate all optimal plans via Greedy Search <a name="all_opt_greedy_plans"></a>
Figure file: N/A</br>
Script file: `scripts_prepare/collect_greedy_plans.py`

Output file JOB-light and JOB:
- `input_data/job/greedy_traversals-opt-cost.csv`
- `input_data/job/greedy_traversals-opt-cost-psql.csv`
- `input_data/job/greedy_traversals-opt-cost-compass.csv`
- `input_data/job_light/greedy_traversals-opt-cost.csv`
- `input_data/job_light/greedy_traversals-opt-cost-psql.csv`
- `input_data/job_light/greedy_traversals-opt-cost-compass.csv`

Input files JOB-light and JOB:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job_light/JOB_light_QUERIES`
- `input_data/job/results_estimates-JOB.csv`
- `input_data/job_light/results_estimates-JOB-light.csv`

## 3. Generate all plans per query (Figure 8) <a name="all_query_plans"></a>
Figure file: N/A</br>
Script file: `scripts_prepare/fig8_benchmark_exhaustive_all_plans.py`</br>
Output file: `input_data/job/exhaustive_all_plans_with_costs.csv`</br>
Input files:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job/results_estimates-JOB.csv`

Exhaustive Search, JOB workload.

## 4. Generate all sql query files per query (Figure 8) <a name="sql_files"></a>
Figure file: N/A</br>
Script file: `scripts_prepare/fig8_generate_fixed_order_queries.py`</br>
Output folder: `input_data/job/FIXED_PLAN_JOB_EXHAUSTIVE_OPT`</br>
Input files:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job/exhaustive_all_plans_with_costs.csv`

Exhaustive Search, JOB workload.

## 5. L1-error features <a name="l1_features"></a>
Figure file: N/A</br>
Script file:
- `scripts/L1_error_job_exhaustive.py`
- `scripts/L1_error_job_greedy.py`
- `scripts/L1_error_job_light_exhaustive.py`
- `scripts/L1_error_job_light_greedy.py`

Output files JOB:
- `input_data/job/L1-errors-agg-exhaustive-psql.csv`
- `input_data/job/L1-errors-agg-exhaustive-compass.csv`
- `input_data/job/L1-errors-agg-greedy-psql.csv`
- `input_data/job/L1-errors-agg-greedy-compass.csv`

Output files JOB-light:
- `input_data/job_light/L1-light-errors-agg-exhaustive-psql.csv`
- `input_data/job_light/L1-light-errors-agg-exhaustive-compass.csv`
- `input_data/job_light/L1-light-errors-agg-greedy-psql.csv`
- `input_data/job_light/L1-light-errors-agg-greedy-compass.csv`

Input files JOB:
- `input_data/job/JOB_QUERIES_COMPASS_PostgreSQL`
- `input_data/job/results_estimates-JOB.csv`
- `input_data/job/exhaustive_traversals-opt-cost.csv`
- `input_data/job/exhaustive_traversals-opt-cost-psql.csv`
- `input_data/job/exhaustive_traversals-opt-cost-compass.csv`
- `input_data/job/greedy_traversals-opt-cost.csv`
- `input_data/job/greedy_traversals-opt-cost-psql.csv`
- `input_data/job/greedy_traversals-opt-cost-compass.csv`

Input files JOB-light:
- `input_data/job_light/JOB_light_QUERIES`
- `input_data/job_light/results_estimates-JOB-light.csv`
- `input_data/job_light/exhaustive_traversals-opt-cost.csv`
- `input_data/job_light/exhaustive_traversals-opt-cost-psql.csv`
- `input_data/job_light/exhaustive_traversals-opt-cost-compass.csv`
- `input_data/job_light/greedy_traversals-opt-cost.csv`
- `input_data/job_light/greedy_traversals-opt-cost-psql.csv`
- `input_data/job_light/greedy_traversals-opt-cost-compass.csv`

PostgreSQL and COMPASS, Greedy and Exhaustive Search, JOB-light and JOB workload.
