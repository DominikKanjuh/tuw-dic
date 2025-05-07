from mrjob.job import MRJob
import json
import re
import os
import mrjob.protocol

# Tokenizacija
TOKEN_REGEX = re.compile(r"[\s()\[\].!?,;:+=\-_\"'`~#@&*%€$§\\/]+")

def load_stopwords(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Stopwords file not found at: {path}")
    with open(path, 'r', encoding='utf-8') as f:
        return set(line.strip().lower() for line in f)

def tokenize(text, stopwords):
    text = text.lower()
    raw_tokens = TOKEN_REGEX.split(text)
    return {tok for tok in raw_tokens if len(tok) > 1 and tok not in stopwords}

class PreprocessJob(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--stopwords', help='Path to stopwords.txt')

    def mapper_init(self):
        self.stopwords = load_stopwords(self.options.stopwords)

    def mapper(self, _, line):
        try:
            review = json.loads(line)
            category = review.get('category', '').strip()
            text = review.get('reviewText', '')

            # izdvajanje jedinstvenih tokena po dokumentu
            unique_tokens = tokenize(text, self.stopwords)

            for token in unique_tokens:
                yield (category, token), 1

            # brojanje dokumenata po kategoriji
            yield ('!DOC_COUNT!', category), 1
        except Exception:
            pass

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        if key[0] == '!DOC_COUNT!':
            yield key, sum(values)
        else:
            category, token = key
            yield (category, token), sum(values)

if __name__ == '__main__':
    PreprocessJob.run()
