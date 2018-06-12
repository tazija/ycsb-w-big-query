#!/usr/bin/env bash

# Print the usage message
function printHelp () {
  echo "Usage: "
  echo "  ./multithreaded_runner.sh -w <workload_name> -d <database_name> -i <client_instances_count> -p arg1=argvalue1 -p arg2=argvalue2"
  echo "  ./multithreaded_runner.sh -h|--help (print this message)"
  echo "    -w <workload> - workload name to launch"
  echo "    -d <workload> - database use. Examples: 'couchbase2' | 'cassandra-cql' | ..."
  echo "    -i <client_instances_count> - client instance count. currently supported values are 1, 2, 3;"
  echo "    -p <ycsb_parameter> - a set of ycsb extra parameters to add. Should be in format '-p name=value'"
}

# Parse commandline args
ARGS=()
while getopts "h?w:d:i:p:" opt; do
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
    THREADS=(25 50 75 100 125 150 200 250 300)
elif [ "${INSTANCES}" -eq "3" ]; then
    THREADS=(15 30 50 75 100 125 150 200)
else
    echo "Instances number is either not specified or not valid (valid values are 1, 2, 3). Aborting evaluation"
    exit 1;
fi

for threads in ${THREADS[@]}; do
 echo " >> Launching workload ${WORKLOAD} in ${threads} threads..."

 echo "mkdir -p ./results/4-nodes/couchbase/${WORKLOAD} && ./bin/ycsb run -s ${DB} -P workloads/${WORKLOAD} -threads ${threads} -p statsexportfile=./results/4-nodes/couchbase/${WORKLOAD}/results.csv ${ARGS[@]}" | sh

 echo " << Finished workload ${WORKLOAD} execution in ${threads} threads!"
done