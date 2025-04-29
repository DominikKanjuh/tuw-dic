import sys
import os

input_dir = sys.argv[1]
output_file = sys.argv[2]

category_lines = {}
all_tokens = set()

for filename in sorted(os.listdir(input_dir)):
    path = os.path.join(input_dir, filename)
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if not parts:
                continue
            first = parts[0]

            if ":" not in parts[1]:
                continue

            category = first
            category_lines[category] = line

            for token_score in parts[1:]:
                token, _ = token_score.split(":")
                all_tokens.add(token)

sorted_categories = sorted(category_lines.keys())
sorted_tokens = sorted(all_tokens)

with open(output_file, "w", encoding="utf-8") as f:
    for category in sorted_categories:
        f.write(category_lines[category] + "\n")
    f.write(" ".join(sorted_tokens) + "\n")
