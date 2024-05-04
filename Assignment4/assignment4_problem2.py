#!/usr/bin/env python3

import time
import argparse
import findspark
findspark.init()
from pyspark import SparkContext


# Define a function to extract the date
def extract_date(line):
    # Find and extract the date part in the log
    date_str = line[:6]
    return date_str

# Define a function to extract the login result
def extract_login_result(line):
    # If it contains "Accepted password for" it's a success, otherwise it's a failure
    if "Accepted password for" in line:
        return 1
    else:
        if "message repeated" in line:
            start_index = line.find("message repeated") + len("message repeated") + 1
            end_index = line.find("time")
            return int(line[start_index:end_index].strip())
        else:
            return 1

# Define a function to extract the account
def extract_account(line):
    # Find the string between "for" and "from" as the account
    start_index = line.find("for") + len("for") + 1
    end_index = line.find("from")
    
    # Extract the string between "for" and "from"
    account_string = line[start_index:end_index].strip()
    
    # If the string contains "invalid user", take the first word after it as the account
    if "invalid user" in account_string:
        invalid_user_index = account_string.find("invalid user") + len("invalid user")
        try:
            account = account_string[invalid_user_index:].strip().split()[0]
        except:
            return None
    # Otherwise, take the last word as the account
    else:
        account = account_string.split()[-1]
    
    return account

# Define a function to extract the invalid account
def extract_invalid_account(line):
    # Find the string between "user" and "from" as the account
    start_index = line.find("invalid user") + len("invalid user") + 1
    end_index = line.find("from")
    return line[start_index:end_index].strip()

# Define a function to extract the IP
def extract_ip(line):
    # Find the string after "from" as the IP
    start_index = line.find("from") + 5
    end_index = line.find("port")
    return line[start_index:end_index].strip()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'SSH Log Analysis.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',default="SSH.log",type=str,help='Input filename')
    args = parser.parse_args()

    # start = time.time()
    sc = SparkContext(master = f'local[{args.num_workers}]')

    lines = sc.textFile(args.filename)
    
    # Filter the lines that contains the longin information
    accepted_lines = lines.filter(lambda line: "Accepted password for" in line)
    failed_lines = lines.filter(lambda line: "Failed password for" in line)
    invalidf_user_lines = lines.filter(lambda line: "Failed password for invalid user" in line)

    # Parse date, login results, account number, IPs and invalid accounts
    dates_accepted = accepted_lines.map(extract_date)
    dates_failed = failed_lines.map(extract_date)
    login_results_accepted = accepted_lines.map(extract_login_result)
    login_results_failed = failed_lines.map(extract_login_result)
    accounts_accepted = accepted_lines.map(extract_account)
    accounts_failed = failed_lines.map(extract_account)
    ips_accepted = accepted_lines.map(extract_ip)
    ips_failed = failed_lines.map(extract_ip)
    invalid_accounts_failed = invalidf_user_lines.map(extract_invalid_account)
    invalid_accounts_failed_attempts = invalidf_user_lines.map(extract_login_result)
    
    # print("Date:", dates_accepted.collect())
    # print("Login Result:", login_results_accepted.collect())
    # print("Account:", accounts_accepted.collect())
    # print("IP:", ips_accepted.collect())
    # print("Date:", dates_failed.collect())
    # print("Login Result:", login_results_failed.collect())
    # print("Account:", accounts_failed.collect())
    # print("IP:", ips_failed.collect())
    # print("Invalid users:", invalid_accounts_failed.collect())
    # print("Invalid users login:", invalid_accounts_failed_attempts.collect())

#----------Subtask: problem(a)---------- 
    # Combine successful and unsuccessful login attempts for each user account
    combined_attempts_accepted = accounts_accepted.zip(login_results_accepted)
    combined_attempts_failed = accounts_failed.zip(login_results_failed)
    combined_attempts = combined_attempts_accepted.union(combined_attempts_failed)

    # Calculate total attempts for each user account
    total_attempts = combined_attempts.reduceByKey(lambda a, b: a + b)
    max_attempts_account = total_attempts.max(lambda x: x[1])
    print('\nproblem: 2-(a)')
    print(f"User '{max_attempts_account[0]}' had {max_attempts_account[1]} login attempts.")


#----------Subtask: problem(b)----------
    # Calculate total successful attempts for each user account
    total_successful_attempts = combined_attempts_accepted.reduceByKey(lambda a, b: a + b)
    # Find the account with the largest number of successful attempts
    max_successful_attempts_account = total_successful_attempts.max(lambda x: x[1])
    print('\nproblem: 2-(b)')
    print(f"User '{max_successful_attempts_account[0]}' had the largest number of successful login attempts, totaling {max_successful_attempts_account[1]} attempts.")


#----------Subtask: problem(c)----------
    # Calculate total unsuccessful attempts for each user account
    total_unsuccessful_attempts = combined_attempts_failed.reduceByKey(lambda a, b: a + b)
    max_unsuccessful_attempts_account = total_unsuccessful_attempts.max(lambda x: x[1])
    print('\nproblem: 2-(c)')
    print(f"User '{max_unsuccessful_attempts_account[0]}' had the largest number of unsuccessful login attempts, totaling {max_unsuccessful_attempts_account[1]} attempts.")
   
#----------Subtask: problem(d)----------
    # Calculate success rates for each user account
    success_rates = total_successful_attempts.leftOuterJoin(total_attempts).map(lambda x: (x[0], x[1][0] / x[1][1]))
    max_success_rate = success_rates.max(lambda x: x[1])
    accounts_with_max_success_rate = success_rates.filter(lambda x: x[1] == max_success_rate[1]).collect()
    print("\nproblem: 2-(d)")
    for account in accounts_with_max_success_rate:
        print(f"User '{account[0]}' had the highest success rate of login at {account[1]*100:.2f}%.")

#----------Subtask: problem(e)----------
    # Calculate total failed login attempts for each IP address
    failed_login_attempts_by_ip = ips_failed.zip(login_results_failed).reduceByKey(lambda a, b: a + b)
    top_failed_ip_addresses = failed_login_attempts_by_ip.takeOrdered(3, key=lambda x: -x[1])
    print('\nproblem: 2-(e)')
    print("Top 3 IP addresses with the largest number of failed login attempts:")
    for ip, attempts in top_failed_ip_addresses:
        print(f"IP: {ip}, number of failed attempts: {attempts}.")
         
#----------Subtask: problem(f)----------
    # Calculate total failed login attempts for each invalid user account
    failed_login_attempts_by_invalid_user = invalid_accounts_failed.zip(invalid_accounts_failed_attempts).reduceByKey(lambda a, b: a + b)
    top_failed_accounts = failed_login_attempts_by_invalid_user.takeOrdered(10, key=lambda x: -x[1])
    print('\nproblem: 2-(f)')
    print("Top 10 invalid user accounts with the largest number of failed login attempts:")
    for account, attempts in top_failed_accounts:
        print(f"User: '{account}', number of failed attempts: {attempts}.")

#----------Subtask: problem(g)----------
    # Combine successful and unsuccessful login attempts for each date
    combined_attempts_by_date_accepted = dates_accepted.zip(login_results_accepted)
    combined_attempts_by_date_failed = dates_failed.zip(login_results_failed)
    combined_attempts_by_date = combined_attempts_by_date_accepted.union(combined_attempts_by_date_failed)
    total_attempts_by_date = combined_attempts_by_date.reduceByKey(lambda a, b: a + b)
    max_activity_day = total_attempts_by_date.max(lambda x: x[1])
    min_activity_day = total_attempts_by_date.min(lambda x: x[1])

    print('\nproblem: 2-(g)')
    print(f'{max_activity_day[0]} had the most login activity, with {max_activity_day[1]} attempts.')
    print(f'{min_activity_day[0]} had the least login activity, with {min_activity_day[1]} attempts.')