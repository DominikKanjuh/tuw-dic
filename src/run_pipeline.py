#!/usr/bin/env python3
import subprocess
import os

HADOOP_STREAMING_JAR = "/usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"

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

    cmd = [
        "python", script,
        "-r", "hadoop",
        "--hadoop-streaming-jar", HADOOP_STREAMING_JAR
    ] + extra_args + inputs + ["--output-dir", output_dir]
    
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    print(f"Completed {script}\n")

def fetch_hdfs_dir(hdfs_path, local_dir):
    """Preuzmi direktorijum sa HDFS-a na lokalnu mašinu."""
    os.makedirs(os.path.dirname(local_dir), exist_ok=True)
    subprocess.run(["hdfs", "dfs", "-get", "-f", hdfs_path, local_dir], check=True)


def main():
    clear_output_dirs()

    # 1) Preprocess: token/category pairs
    run_job(
        "src/jobs/preprocess_job.py",
        "hdfs:///user/dic25_shared/amazon-reviews/full/reviewscombined.json",
        "data/intermediate/preprocessed",
        ["--stopwords", "data/input/stopwords.txt"],
    )

    # 2) Count: per-category & per-term-per-category
    run_job(
        "src/jobs/count_job.py",
        "hdfs:///user/e12439367/data/intermediate/preprocessed",
        "data/intermediate/counts",
    )

    # Fetch hdfs directories locally
    fetch_hdfs_dir("hdfs:///user/e12439367/data/intermediate/counts", "data/tmp/counts")
    fetch_hdfs_dir("hdfs:///user/e12439367/data/intermediate/preprocessed", "data/tmp/preprocessed")

    # 3) Merge local results into one file
    merge_dirs_to_file(
        [
            "data/tmp/counts",
            "data/tmp/preprocessed",
        ],
        "data/intermediate/counts_all.txt"
    )
    print("Produced data/intermediate/counts_all.txt\n")

    # 4) Compute χ²: stream only preprocessed through stdin,
    #    ship counts_all.txt as side‐file
    run_job(
        "src/jobs/chi_square_job.py",
        "hdfs:///user/e12439367/data/intermediate/preprocessed",
        "data/intermediate/chi2_scores",
        ["--counts-all", "data/intermediate/counts_all.txt"],
    )

    # 5) Extract top K
    run_job(
        "src/jobs/topk_job.py",
        "hdfs:///user/e12439367/data/intermediate/chi2_scores",
        "data/intermediate/topk_outputs",
    )
    # 6) Fetch top-K HDFS results locally
    fetch_hdfs_dir(
        "hdfs:///user/e12439367/data/intermediate/topk_outputs",
        "data/tmp/topk_outputs"
    )
    
    # 7) Run script for final merging and formating
    subprocess.run(
        [
            "python",
            "src/scripts/merge_topk_outputs.py",
            "data/tmp/topk_outputs",
            "data/output/output.txt",
        ],
        check=True,
    )


    print("Pipeline complete!")

if __name__ == "__main__":
    main()
