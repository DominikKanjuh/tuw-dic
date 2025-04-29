from mrjob.job import MRJob
import mrjob.protocol


class TopKJob(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def mapper(self, key, value):
        category, token = key
        yield category, (token, value)

    def reducer(self, category, token_values):
        top_tokens = sorted(token_values, key=lambda x: -x[1])[:75]
        yield category, " ".join(f"{token}:{chi2:.2f}" for token, chi2 in top_tokens)


if __name__ == "__main__":
    TopKJob.run()
