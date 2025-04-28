from mrjob.job import MRJob


class ChiSquareJob(MRJob):
    def mapper(self, key, value):
        # Dummy chi-square logic
        yield "chi2", (key, value)


if __name__ == "__main__":
    ChiSquareJob.run()
