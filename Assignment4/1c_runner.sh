#!/bin/bash

for i in {1,2,4,8,16,32}
do
    /opt/local/bin/run_job.sh -p cpu-shannon --cpus-per-task $i --env pyspark --script assignment4_problem1c.py -- /bayes_data/2024-DAT470-DIT065/twitter-2010_10M.txt -w $i
done