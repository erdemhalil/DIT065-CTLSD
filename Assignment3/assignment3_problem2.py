#!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRJobTwitterFollowers(MRJob):
    def mapper_get_followers(self, _, line: str):
        user, followers = line.split(':')
        followers: list = followers.strip().split()
        yield user, 0   # user has no followers
        for follower in followers:
            yield follower, 1
    
    def combiner_count_followers(self, user, counts):
        yield user, sum(counts)

    def reducer_count_followers(self, user, counts):
        yield None, (user, sum(counts))

    def reducer_final_results(self, _, users_and_counts):
        total_followers = 0
        total_users = 0
        max_followers = 0
        max_user = None
        no_followers = 0

        for user, count in users_and_counts:
            total_followers += count
            total_users += 1
            if count > max_followers:
                max_followers = count
                max_user = user
            if count == 0:
                no_followers += 1

        yield 'most followers id', max_user
        yield 'most followers', max_followers
        yield 'average followers', total_followers / total_users
        yield 'count no followers', no_followers

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_followers,
                    combiner=self.combiner_count_followers,
                    reducer=self.reducer_count_followers),
            MRStep(reducer=self.reducer_final_results)
        ]

if __name__ == '__main__':
    MRJobTwitterFollowers.run()
