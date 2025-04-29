from mrjob.job import MRJob
import mrjob.protocol


class CountJob(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def mapper(self, key, value):
        category, token = key
        yield (category, token), value

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    CountJob.run()
