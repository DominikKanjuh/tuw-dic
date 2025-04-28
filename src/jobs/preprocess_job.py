from mrjob.job import MRJob


class PreprocessJob(MRJob):
    def configure_args(self):
        super().configure_args()
        # Add your args like --stopwords here

    def mapper(self, _, line):
        # Dummy mapper logic
        yield "preprocessed", line


if __name__ == "__main__":
    PreprocessJob.run()
