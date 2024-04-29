#!/bin/bash

for i in {1..99}
do 
    python blackjack.py 100000 -w $i --seed 42
done