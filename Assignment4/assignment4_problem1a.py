#!/usr/bin/env python3

import time
import argparse
import findspark
findspark.init()
from pyspark import SparkContext


def extract_follows(line: str):
    parts = line.split(": ")
    user_id = parts[0].strip()
    followers = parts[1].split()
    return user_id, len(followers)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = \
                                    'Compute Twitter follows.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    start = time.time()
    sc = SparkContext(master = f'local[{args.num_workers}]')

    lines = sc.textFile(args.filename)

    user_follows = lines.map(extract_follows)

    # Find the user that follows the most people
    max_follows = user_follows.reduce(lambda a, b: a if a[1] > b[1] else b)

    # Calculate the average number of follows
    total_follows = user_follows.map(lambda x: x[1]).sum()
    total_users = user_follows.count()
    avg_follows = total_follows / total_users

    # Count the number of users who follow no one
    no_follows = user_follows.filter(lambda x: x[1] == 0).count()

    end = time.time()
    
    total_time = end - start

    # Output results
    print(f'max follows: {max_follows[0]} follows {max_follows[1]}')
    print(f'users follow on average: {avg_follows}')
    print(f'number of user who follow no-one: {no_follows}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time}')