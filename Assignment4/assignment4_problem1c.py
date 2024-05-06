#!/usr/bin/env python3

import time
import argparse
import findspark
findspark.init()
from pyspark import SparkContext

def parse_line(line):
    parts = line.split(': ')
    user_id = parts[0].strip()
    followers = [x.strip() for x in parts[1].split()]
    return user_id, followers

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = \
                                    'Compute Twitter followers.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    start = time.time()
    sc = SparkContext(master = f'local[{args.num_workers}]')

    lines = sc.textFile(args.filename)

    # Parse each line to extract user ID and followers
    parsed_lines = lines.map(parse_line)

    # Flatten the list of followers and count occurrences of each user ID
    follower_counts = parsed_lines.flatMap(lambda x: x[1]) \
                                  .map(lambda x: (x, 1)) \
                                  .reduceByKey(lambda a, b: a + b)

    # Find the user ID with the maximum number of followers
    max_followers = follower_counts.reduce(lambda a, b: a if a[1] > b[1] else b)

    # Calculate average number of followers
    total_followers = follower_counts.map(lambda x: x[1]).sum()
    num_users = parsed_lines.map(lambda x: x[0]).distinct().count()  # Count distinct user IDs
    avg_followers = total_followers / num_users

    # Count number of users with no followers
    num_no_followers = num_users - follower_counts.count()

    end = time.time()
    
    total_time = end - start

    # Output results
    print(f'max followers: {max_followers[0]} has {max_followers[1]} followers')
    print(f'followers on average: {avg_followers}')
    print(f'number of user with no followers: {num_no_followers}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time}')
