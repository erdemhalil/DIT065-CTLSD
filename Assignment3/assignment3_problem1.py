 #!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRJobTwitterFollows(MRJob):        
    def mapper(self, _, line):
        # Skip empty lines
        if not line:
            return

        user, follows = line.split(': ')
        follows_list = follows.split()
        yield (user, len(follows_list))

    def combiner(self, user, follows_count):
        yield (user, sum(follows_count))

    def reducer(self, user, follows_counts):
        yield (None, (user, sum(follows_counts)))

    def reducer_max_and_average(self, _, user_follows_counts):
        max_follows_user = float('inf')
        max_follows_count = 0
        total_follows_count = 0
        total_users = 0
        no_follows_users = 0

        for user, follows_count in user_follows_counts:
            total_follows_count += follows_count
            total_users += 1
            if follows_count > max_follows_count:
                max_follows_count = follows_count
                max_follows_user = user
            if follows_count == 0:
                no_follows_users += 1

        average_follows = total_follows_count / total_users if total_users > 0 else 0

        yield ('most followed id', max_follows_user)
        yield ('most followed', max_follows_count)
        yield ('average followed', average_follows)
        yield ('count follows no-one', no_follows_users)

    def steps(self):
        return [MRStep(mapper=self.mapper,combiner=self.combiner,reducer=self.reducer),
                MRStep(reducer=self.reducer_max_and_average)]

if __name__ == '__main__':
    MRJobTwitterFollows.run()
