from mrjob.job import MRJob


class CountJob(MRJob):
    def mapper(self, _, line):
        # Dummy mapper logic
        yield line, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    CountJob.run()
