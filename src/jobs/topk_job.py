from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
from collections import defaultdict
import mrjob.protocol


class TopKJob(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    def mapper(self, key, value):
        if isinstance(key, str):
            key = ast.literal_eval(key)
        if isinstance(value, str):
            value = float(value)
        category, token = key
        yield category, (token, value)

    def reducer_init(self):
        self.category_to_tokens = defaultdict(list)

    def reducer(self, category, token_values):
        self.category_to_tokens[category].extend(token_values)

    def reducer_final(self):
        all_terms = set()
        sorted_categories = sorted(self.category_to_tokens.keys())
        for category in sorted_categories:
            top_tokens = sorted(self.category_to_tokens[category], key=lambda x: -x[1])[
                :75
            ]
            formatted_tokens = " ".join(
                f"{token}:{score:.2f}" for token, score in top_tokens
            )
            yield None, f"{category} {formatted_tokens}"
            all_terms.update(token for token, _ in top_tokens)

        merged_line = " ".join(sorted(all_terms))
        yield None, merged_line

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer_init=self.reducer_init,
                reducer=self.reducer,
                reducer_final=self.reducer_final,
            )
        ]


if __name__ == "__main__":
    TopKJob.run()
