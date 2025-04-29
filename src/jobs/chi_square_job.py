from mrjob.job import MRJob
from collections import defaultdict
import mrjob.protocol


class ChiSquareJob(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def mapper(self, key, value):
        category, token = key
        yield token, (category, value)

    def reducer(self, token, values):
        total_count = defaultdict(int)
        for category, count in values:
            total_count[category] += count

        for category, cat_count in total_count.items():
            A = cat_count
            B = sum(total_count.values()) - A
            chi2 = (A - B) ** 2 / (A + B) if (A + B) > 0 else 0
            yield (category, token), chi2


if __name__ == "__main__":
    ChiSquareJob.run()
