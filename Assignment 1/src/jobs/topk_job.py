#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol, RawValueProtocol
import heapq

class Top75TermsPerCategory(MRJob):
    # read JSON-encoded ([term,category], chi2) pairs
    INPUT_PROTOCOL  = JSONProtocol
    # emit raw text lines
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, key, value):
        # key == [term, category], value == chi2
        term, category = key      # first element is term, second is category
        chi2 = float(value)
        # emit by category so reducer can collect all (chi2,term)
        yield category, (chi2, term)

    def reducer(self, category, values):
        # keep a min-heap of size 75 for this category
        h = []
        for chi2, term in values:
            if len(h) < 75:
                heapq.heappush(h, (chi2, term))
            else:
                heapq.heappushpop(h, (chi2, term))

        # sort descending by chi2 and format
        top75 = sorted(h, key=lambda x: -x[0])
        pairs = [f"{term}:{chi2}" for chi2, term in top75]
        # wrap category in <> and emit one line
        yield None, f"<{category}> " + " ".join(pairs)

if __name__ == '__main__':
    Top75TermsPerCategory.run()
