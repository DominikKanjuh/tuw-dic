import subprocess
import argparse
import os
import shutil


def clean_local_dirs():
    paths = ["data/intermediate", "data/output"]
    for path in paths:
        if os.path.exists(path):
            shutil.rmtree(path)
            os.makedirs(path)


def run_job(script, input_path, output_path, runner_args):
    cmd = ["python", script] + runner_args + [input_path, "--output-dir", output_path]
    subprocess.run(cmd, check=True)
    print(f"Completed {script}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Run locally")
    parser.add_argument("--cluster", action="store_true", help="Run on Hadoop cluster")
    parser.add_argument(
        "--input", required=True, help="Input reviews file (local or HDFS)"
    )
    parser.add_argument(
        "--stopwords", required=True, help="Stopwords file (local or HDFS)"
    )
    parser.add_argument(
        "--output", required=True, help="Base output path (local or HDFS)"
    )
    args = parser.parse_args()

    if args.local == args.cluster:
        raise ValueError("Specify exactly one of --local or --cluster.")

    if args.local:
        print("Running locally...")
        clean_local_dirs()
        runner_args = []
    else:
        print("Running on Hadoop cluster...")

        # Clean previous outputs if they exist
        for dir_name in ["preprocessed", "counts", "chi2", "topk"]:
            subprocess.run(
                ["hadoop", "fs", "-rm", "-r", f"{args.output}/{dir_name}"],
                stderr=subprocess.DEVNULL,
            )

        runner_args = [
            "-r",
            "hadoop",
            "--hadoop-streaming-jar",
            "/usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar",
            "--jobconf",
            "mapreduce.job.reduces=5",
        ]

    run_job(
        "src/jobs/preprocess_job.py",
        args.input,
        os.path.join(args.output, "preprocessed"),
        runner_args + ["--stopwords", args.stopwords],
    )

    run_job(
        "src/jobs/count_job.py",
        os.path.join(args.output, "preprocessed"),
        os.path.join(args.output, "counts"),
        runner_args,
    )

    run_job(
        "src/jobs/chi_square_job.py",
        os.path.join(args.output, "counts"),
        os.path.join(args.output, "chi2"),
        runner_args,
    )

    run_job(
        "src/jobs/topk_job.py",
        os.path.join(args.output, "chi2"),
        os.path.join(args.output, "topk"),
        runner_args,
    )

    subprocess.run(
        [
            "python",
            "src/scripts/merge_topk_outputs.py",
            ("data/intermediate/topk" if args.local else f"{args.output}/topk"),
            ("data/output/output.txt" if args.local else "output.txt"),
        ]
        + (["--local"] if args.local else []),
        check=True,
    )

    print(
        f"Output written to {'data/output/output.txt' if args.local else 'output.txt'}"
    )

    print("Pipeline complete!")


if __name__ == "__main__":
    main()
