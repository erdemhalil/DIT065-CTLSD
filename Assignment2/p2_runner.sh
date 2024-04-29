#!/bin/bash

for i in {1..99}
do
   python ufo.py -w $i -q test_data_assignment2_problem2/queries_verylarge.txt
done