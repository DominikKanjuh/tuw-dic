#!/usr/bin/env python3
import os
import json
import math
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, JSONProtocol

class ChiSquareJob(MRJob):
    # Read raw lines from stdin (JSON_key \t count)
    INPUT_PROTOCOL  = RawValueProtocol
    # Emit JSON-encoded (term, category) → chi2_value
    OUTPUT_PROTOCOL = JSONProtocol

    def configure_args(self):
        super().configure_args()
        self.add_file_arg(
            '--counts-all',
            help='Merged counts file (per-category counts, global term counts, total docs)'
        )

    def mapper_init(self):
        # counts_all.txt is shipped as a side-file; open by basename
        counts_path = os.path.basename(self.options.counts_all)

        self.total_docs  = 0
        self.cat_totals  = {}  # category → total docs in that category
        self.term_totals = {}  # term     → total docs containing that term

        with open(counts_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    key_str, val_str = line.split('\t', 1)
                    key   = json.loads(key_str)
                    count = int(val_str)
                except:
                    continue

                # per-category doc counts
                if isinstance(key, list) and key[0] == "!DOC_COUNT!":
                    cat = key[1]
                    self.cat_totals[cat] = count
                    self.total_docs    += count

                # global term counts (ignore TOTAL_DOC_COUNT if present)
                elif isinstance(key, str) and key != "TOTAL_DOC_COUNT":
                    term = key
                    self.term_totals[term] = count

                # skip the ["category","term"] entries here

    def mapper(self, _, raw_line):
        # raw_line should be ["Category","term"]\tA
        line = raw_line.strip()
        if not line:
            return

        try:
            key_str, val_str = line.split('\t', 1)
            key = json.loads(key_str)
            A   = int(val_str)
        except:
            return

        # only process term-in-category entries
        if not (isinstance(key, list) and len(key) == 2 and key[0] != "!DOC_COUNT!"):
            return

        category, term = key
        N          = self.total_docs
        term_total = self.term_totals.get(term, 0)
        cat_total  = self.cat_totals .get(category, 0)

        B = term_total - A
        C = cat_total  - A
        D = N - A - B - C

        numerator   = (A * D - B * C) ** 2
        denominator = (A + C) * (B + D) * (A + B) * (C + D)
        if denominator > 0:
            chi2 = N * numerator / denominator
            yield (term, category), chi2

    def reducer(self, key, values):
        # sum in case of combiners; typically one value per key
        yield key, sum(values)

if __name__ == "__main__":
    ChiSquareJob.run()
