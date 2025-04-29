#!/usr/bin/env python3
import subprocess
import os

def clear_output_dirs():
    subprocess.run(["python", "src/scripts/clear_dirs.py"], check=True)
    print("Cleared intermediate and output directories")

def merge_dirs_to_file(directories, output_file):
    """
    Concatenate all part-* files under each directory in `directories`
    into a single flat text file at `output_file`.
    """
    with open(output_file, "w", encoding="utf-8") as fout:
        for directory in directories:
            if not os.path.isdir(directory):
                continue
            for fname in sorted(os.listdir(directory)):
                fpath = os.path.join(directory, fname)
                if not os.path.isfile(fpath):
                    continue
                with open(fpath, "r", encoding="utf-8") as fin:
                    for line in fin:
                        fout.write(line)

def run_job(script, input_files, output_dir, extra_args=None):
    """
    Runs an MRJob.
    - script: path to the MRJob script
    - input_files: single path or list of paths (streamed over stdin)
    - output_dir: where MRJob writes its part-* files
    - extra_args: list of flags, e.g. ["--counts-all", "foo.txt"]
    """
    if extra_args is None:
        extra_args = []

    if isinstance(input_files, str):
        inputs = [input_files]
    else:
        inputs = input_files

    cmd = ["python", script, "-r", "inline"] + extra_args + inputs + ["--output-dir", output_dir]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    print(f"Completed {script}\n")

def main():
    clear_output_dirs()

    # 1) Preprocess: token/category pairs
    run_job(
        "src/jobs/preprocess_job.py",
        "data/input/reviews_devset.json",
        "data/intermediate/preprocessed",
        ["--stopwords", "data/input/stopwords.txt"],
    )

    # 2) Count: per-category & per-term-per-category
    run_job(
        "src/jobs/count_job.py",
        "data/intermediate/preprocessed",
        "data/intermediate/counts",
    )

    # 3) Merge those two small Hadoop outputs *locally*
    merge_dirs_to_file(
        [
            "data/intermediate/counts",       # per-category + global counts
            "data/intermediate/preprocessed", # per-term-per-category counts
        ],
        "data/intermediate/counts_all.txt"
    )
    print("Produced data/intermediate/counts_all.txt\n")

    # 4) Compute χ²: stream only preprocessed through stdin,
    #    ship counts_all.txt as side‐file
    run_job(
        "src/jobs/chi_square_job.py",
        "data/intermediate/preprocessed",
        "data/intermediate/chi2_scores",
        ["--counts-all", "data/intermediate/counts_all.txt"],
    )

    # 5) Extract top K
    run_job(
        "src/jobs/topk_job.py",
        "data/intermediate/chi2_scores",
        "data/intermediate/topk_outputs",
    )

    # 6) Merge top-K parts into final output.txt
    subprocess.run(
        [
            "python",
            "src/scripts/merge_topk_outputs.py",
            "data/intermediate/topk_outputs",
            "data/output/output.txt",
        ],
        check=True,
    )

    print("Pipeline complete!")

if __name__ == "__main__":
    main()
