#!/bin/bash

for i in {1,2,4,8,16,32}
do
    /opt/local/bin/run_job.sh -p cpu-markov --cpus-per-task $i --env mrjob --script assignment3_problem2.py -- /bayes_data/2024-DAT470-DIT065/twitter-2010_10M.txt -w $i
done