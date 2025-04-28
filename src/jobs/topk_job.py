from mrjob.job import MRJob


class TopKJob(MRJob):
    def mapper(self, key, value):
        # Dummy top-k logic
        yield "topk", (key, value)


if __name__ == "__main__":
    TopKJob.run()
