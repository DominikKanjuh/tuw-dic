#!/usr/bin/env python3
from mrjob.job import MRJob
import json

class CountJob(MRJob):
    """
    Reads counts.txt lines like:
       ["!DOC_COUNT!","Books"]    1234
       ["Books","amazing"]        567
       "amazing"                  3456
       "TOTAL_DOC_COUNT"          100000

    And emits:
       "TOTAL_DOC_COUNT" → sum of all !DOC_COUNT! values
       "<term>"         → sum of all counts for that term
    """

    def mapper(self, _, line):
        line = line.strip()
        if not line:
            return

        # split into JSON-key and int count
        try:
            key_str, val_str = line.split('\t', 1)
            count = int(val_str)
            key   = json.loads(key_str)
        except Exception:
            return

        # 1) category doc counts → TOTAL_DOC_COUNT
        if isinstance(key, list) and key[0] == "!DOC_COUNT!":
            yield "TOTAL_DOC_COUNT", count

        # 2) term-category counts → global term counts
        elif isinstance(key, list) and len(key) == 2:
            category, term = key
            yield term, count

        # 3) standalone global-term counts or literal TOTAL_DOC_COUNT
        elif isinstance(key, str):
            yield key, count

    def combiner(self, key, counts):
        # local aggregation
        yield key, sum(counts)

    def reducer(self, key, counts):
        # final aggregation
        yield key, sum(counts)
        

if __name__ == "__main__":
    CountJob.run()
