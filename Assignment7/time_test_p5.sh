#!/bin/bash

datasets=('pubs.csv' 'glove.6B.50d.txt' 'glove.840B.300d.txt')
log_file="a7_p5_a_output.log"

> $log_file

for dataset in "${datasets[@]}"
do
    for size in 'tiny' 'small' 'medium' 'big'
    do
        if [ "$size" == "tiny" ]; then
            batch_size=5
        elif [ "$size" == "small" ]; then
            batch_size=10
        elif [ "$size" == "medium" ]; then
            batch_size=50
        elif [ "$size" == "big" ]; then
            batch_size=100
        fi

        echo "Executing with dataset: $dataset, size: $size, batch size: $batch_size" | tee -a $log_file

        if [[ $dataset == *pub* ]]; then
            python assignment7_problem4.py -d $dataset -q pub_queries_${size}.txt -l pub_queries_${size}_names.txt -b ${batch_size} >> $log_file 2>&1
        fi

        if [[ $dataset == *glove* ]]; then
            dataset_name="${dataset%.*}"
            python assignment7_problem4.py -d $dataset -q ${dataset_name}_queries_${size}.txt -l ${dataset_name}_queries_${size}_names.txt -b ${batch_size} >> $log_file 2>&1
        fi

    done
done


