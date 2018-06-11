#!/usr/bin/env bash

workload=$1 # workloada_couchbase_custom
for threads in 10 20 30 40 50 60 70 80 90 100 110 120 130 140 150 160 170 180 190 200 210 220 230 240 250; do
 echo " >> Launching workload $workload in $threads threads..."

 echo "./bin/ycsb run -s couchbase2 -P workloads/${workload} -threads ${threads} -p statsexportfile=./results/4-nodes/couchbase/${workload}/${threads}_threads.csv" | sh

 echo " << Finished workload $workload execution in $threads threads!"
done