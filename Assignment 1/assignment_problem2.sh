#!/bin/bash

# CPU Information
cpu_model=$(lscpu | grep 'Model name:' | awk -F ': ' '{print $2}' | xargs)
cpu_frequency=$(grep "model name" /proc/cpuinfo | head -n 1 | awk -F ': ' '{print $2}')
cpu_sockets=$(lscpu | grep 'Socket(s):' | awk -F ': ' '{print $2}' | xargs)
cpu_cores_per_socket=$(lscpu | grep 'Core(s) per socket:' | awk -F ': ' '{print $2}' | xargs)
cpu_total_cores=$((cpu_cores_per_socket * cpu_sockets))
cpu_threads_per_core=$(lscpu | grep 'Thread(s) per core:' | awk -F ': ' '{print $2}' | xargs)
cpu_total_threads=$((cpu_total_cores * cpu_threads_per_core))
cpu_instruction_set=$(lscpu | grep 'Architecture:' | awk -F ': ' '{print $2}' | xargs)
cache_line_length=$(getconf LEVEL1_DCACHE_LINESIZE)
l1_cache=$(lscpu | grep 'L1d cache:' | awk -F ': ' '{print $2}' | awk -F ' ' '{print $1,$2}')
l2_cache=$(lscpu | grep 'L2 cache:' | awk -F ': ' '{print $2}' | awk -F ' ' '{print $1,$2}')
l3_cache=$(lscpu | grep 'L3 cache:' | awk -F ': ' '{print $2}' | awk -F ' ' '{print $1,$2}')

# Memory Information
ram_total=$(free -h | grep "Mem:" | awk '{print $2}')

# GPU Information
gpu_count=$(nvidia-smi --list-gpus | wc -l)
gpu_model=$(nvidia-smi --query-gpu=name --format=csv,noheader)

# GPU Memory Information
gpu_memory=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader)

# Filesystem Information
data_fs_type=$(df -T /data | awk 'NR==2 {print $2}')
data_backup_fs_type=$(df -T /datainbackup | awk 'NR==2 {print $2}')
data_total_space=$(df -h /data | awk 'NR==2 {print $2}')
data_free_space=$(df -h /data | awk 'NR==2 {print $4}')
data_backup_total_space=$(df -h /datainbackup | awk 'NR==2 {print $2}')
data_backup_free_space=$(df -h /datainbackup | awk 'NR==2 {print $4}')

# Kernel and Distribution Information
kernel_version=$(uname -r)
distro_version=$(cat /etc/*release | grep PRETTY_NAME | cut -d '=' -f 2 | tr -d '"')

# Python Information
python_filename=$(which python3)
python_version=$(python3 --version)

# Print collected information
echo "###### CPU Information ######"
echo "CPU Model: $cpu_model"
echo "CPU Frequency (MHz): $cpu_frequency"
echo "Number of Physical CPUs (sockets in use): $cpu_sockets"
echo "Number of Cores: $cpu_total_cores"
echo "Number of Hardware Threads: $cpu_total_threads"
echo "Instruction Set Architecture: $cpu_instruction_set"
echo "Cache Line Length: $cache_line_length"
echo "L1 Cache: $l1_cache"
echo "L2 Cache: $l2_cache"
echo "L3 Cache: $l3_cache"

echo -e "\n###### Memory Information ######"
echo "Total RAM: $ram_total"

echo -e "\n###### GPU Information ######"
echo "Number of GPUs: $gpu_count"
echo "GPU Model: $gpu_model"
echo "GPU Memory: $gpu_memory"

echo -e "\n###### Filesystem Information ######"
echo "Filesystem of /data: $data_fs_type"
echo "Filesystem of /datainbackup: $data_backup_fs_type"
echo "Total Space of /data: $data_total_space"
echo "Free Space of /data: $data_free_space"
echo "Total Space of /datainbackup: $data_backup_total_space"
echo "Free Space of /datainbackup: $data_backup_free_space"

echo -e "\n###### Kernel and Distribution Information ######"
echo "Linux Kernel Version: $kernel_version"
echo "Linux Distribution Version: $distro_version"

echo -e "\n###### Python Information ######"
echo "Python Filename: $python_filename"
echo "Python Version: $python_version"
