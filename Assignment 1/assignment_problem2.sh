#!/bin/bash

# Subproblem-a
# Print CPU model
echo "CPU Model:"
lscpu | grep "Model name" || cat /proc/cpuinfo | grep "model name" | uniq

# Print CPU Max Frequency
echo "CPU Max Frequency:"
lscpu | grep "CPU max MHz" || cat /proc/cpuinfo | grep "cpu MHz" | uniq

