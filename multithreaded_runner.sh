#!/usr/bin/env bash

# Print the usage message
function printHelp () {
  echo "Usage: "
  echo "  ./multithreaded_runner.sh -w <workload_name> -d <database_name> -i <client_instances_count> -r <result_file_name> -p arg1=argvalue1 -p arg2=argvalue2"
  echo "  ./multithreaded_runner.sh -h|--help (print this message)"
  echo "    -w <workload> - workload name to launch"
  echo "    -d <workload> - database use. Examples: 'couchbase2' | 'cassandra-cql' | ..."
  echo "    -i <client_instances_count> - client instance count. currently supported values are 1, 2, 3, 4, 5, 6, 7, 10;"
  echo "    -r <result_file_name> - [Optional] result file name. If not specified, defaults to 'results'"
  echo "    -p <ycsb_parameter> - a set of ycsb extra parameters to add. Should be in format '-p name=value'"
}

# Parse commandline args
ARGS=()
RESULT_FILE="results"
while getopts "h?w:d:i:r:p:" opt; do
  case "$opt" in
    h|\?)
      printHelp
      exit 0
    ;;
    w)  WORKLOAD=$OPTARG    # workload name
    ;;
    d)  DB=$OPTARG
    ;;
    i)  INSTANCES=$OPTARG
    ;;
    r)  RESULT_FILE=$OPTARG
    ;;
    p)  ARGS+=("-p $OPTARG")
    ;;
  esac
done

if [ -z "${WORKLOAD}" ]; then
    echo "Workload -w parameter is mandatory! Please, specify it."
    exit 1;
fi

if [ -z "${DB}" ]; then
    echo "Database -d parameter is mandatory! Please, specify it. Examples: 'couchbase2' | 'cassandra-cql' | ..."
    exit 1;
fi

if [ "${INSTANCES}" -eq "1" ]; then
    THREADS=(25 50 75 100 125 150 175 200 250 300 400 500)
elif [ "${INSTANCES}" -eq "2" ]; then
    THREADS=(25 50 75 100 125 150 200 250 300 350)
elif [ "${INSTANCES}" -eq "3" ]; then
    THREADS=(15 30 50 75 100 125 150 200 250)
elif [ "${INSTANCES}" -eq "4" ]; then
    THREADS=(10 25 35 50 75 100 125 150 175)
elif [ "${INSTANCES}" -eq "5" ]; then
    THREADS=(10 20 30 40 50 60 70 80 100 120 140)
#    THREADS=(20 40 60 80 100 120)
elif [ "${INSTANCES}" -eq "-5" ]; then
#    THREADS=(10 20 30 40 50 60 70 80 100 120 140)
    THREADS=(20 40 60 80 100 120)
elif [ "${INSTANCES}" -eq "6" ]; then
    THREADS=(10 20 30 40 50 60 70 80 100 120)
elif [ "${INSTANCES}" -eq "7" ]; then
    THREADS=(5 10 15 20 25 30 40 50 60 70 80 100)
elif [ "${INSTANCES}" -eq "10" ]; then
    THREADS=(5 10 15 20 25 30 40 50 60 70)
else
    echo "Instances number is either not specified or not valid (valid values are 1, 2, 3, 4, 5, 6, 7, 10). Aborting evaluation"
    exit 1;
fi

for threads in ${THREADS[@]}; do
 echo " >> Launching workload ${WORKLOAD} in ${threads} threads..."

 echo "mkdir -p ./results/4-nodes/${INSTANCES}-clients/${DB}/${WORKLOAD} && ./bin/ycsb run -s ${DB} -P workloads/${WORKLOAD} -threads ${threads} -p statsexportfile=./results/4-nodes/${INSTANCES}-clients/${DB}/${WORKLOAD}/${RESULT_FILE}.csv ${ARGS[@]}" | sh

 echo " << Finished workload ${WORKLOAD} execution in ${threads} threads!"
done