#!/bin/bash

# enter L1_error_indicator
# screen -S docker_psql -dm -L sh -c 'time scripts_prepare/./fig8_benchmark_psql_runtime.sh'
# rm screenlog.0

container_name=l1_error_pg_docker

input_folder=input_data/job/FIXED_PLAN_JOB_EXHAUSTIVE_OPT
output_folder=input_data/job/RESULTS_FIXED_PLAN_JOB_EXHAUSTIVE_OPT

psql_key=""
# psql_key="EXPLAIN ANALYZE"
# psql_key="EXPLAIN"

# timing_enabled=""
timing_enabled="\timing"

iterations=5
docker_server_run="docker exec -i ${container_name} psql -h localhost -U postgres -d imdb"
# docker_server_run="docker exec -i ${container_name} psql -h localhost -U postgres -d imdb_pk"
# docker_server_run="docker exec -i ${container_name} psql -h localhost -U postgres -d imdb_pk_fk"

echo "starting the query runs: " -- `date +"%Y-%m-%d %T"`
echo -e "\n"
for query_file in $(ls ${input_folder})
do
  echo ${query_file} -- `date +"%Y-%m-%d %T"`
  query_result=${output_folder}/${query_file}.result
  rm ${query_result}
  touch ${query_result}

  client_query=${output_folder}/${query_file}
  rm ${client_query}
  touch ${client_query}
  echo "${timing_enabled} " > ${client_query}
  echo "${psql_key} " >> ${client_query}
  cat "${input_folder}/${query_file}" >> ${client_query}
  chmod +x ${client_query}

  iter=1
  while [ $iter -le ${iterations} ]
  do
    (${docker_server_run} < ${client_query} >> ${query_result}) & P0=$!
    wait $P0
    ((iter++))
  done;

  rm ${client_query}
done;

echo -e "\nfinishing the query runs: " -- `date +"%Y-%m-%d %T"`
