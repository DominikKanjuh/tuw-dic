import os
import subprocess
import tempfile
import argparse


def parse_and_merge(input_dir, output_file):
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
                if not parts or ":" not in parts[1]:
                    continue
                category = parts[0]
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
    print(f"Merged output written to {output_file}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_path", help="Input directory (local or HDFS)")
    parser.add_argument("output_file", help="Merged output file path")
    parser.add_argument("--local", action="store_true", help="Use local filesystem")
    args = parser.parse_args()

    if args.local:
        parse_and_merge(args.input_path, args.output_file)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.run(
                ["hadoop", "fs", "-get", args.input_path, tmpdir], check=True
            )
            local_dir = os.path.join(tmpdir, os.path.basename(args.input_path))
            parse_and_merge(local_dir, args.output_file)


if __name__ == "__main__":
    main()
