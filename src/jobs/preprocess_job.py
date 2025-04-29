from mrjob.job import MRJob
import re
import json
import mrjob.protocol

WORD_RE = re.compile(r"[\s\t\d\(\)\[\]\{\}\.!\?,;:+=\-_\"'`~#@&*%\u20AC\$\u00A7\\/]+")


class PreprocessJob(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def configure_args(self):
        super().configure_args()
        self.add_file_arg("--stopwords")

    def load_stopwords(self):
        with open(self.options.stopwords, "r") as f:
            return set(word.strip().lower() for word in f.readlines())

    def mapper_init(self):
        self.stopwords = self.load_stopwords()

    def mapper(self, _, line):
        review = json.loads(line)
        category = review.get("category")
        text = review.get("reviewText", "").lower()
        tokens = WORD_RE.split(text)
        for token in tokens:
            if token and len(token) > 1 and token not in self.stopwords:
                yield (category, token), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    PreprocessJob.run()
