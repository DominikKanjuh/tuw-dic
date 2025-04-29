import subprocess


def run_job(script, input_file, output_dir, extra_args=[]):
    cmd = ["python", script] + extra_args + [input_file, "--output-dir", output_dir]
    subprocess.run(cmd, check=True)
    print(f"Completed {script}")


def clear_output_dirs():
    subprocess.run(["python", "src/scripts/clear_dirs.py"], check=True)
    print("Cleared intermediate and output directories")


def main():
    clear_output_dirs()

    run_job(
        "src/jobs/preprocess_job.py",
        "data/input/reviews_devset.json",
        "data/intermediate/preprocessed",
        ["--stopwords", "data/input/stopwords.txt"],
    )
    run_job(
        "src/jobs/count_job.py",
        "data/intermediate/preprocessed",
        "data/intermediate/counts",
    )
    run_job(
        "src/jobs/chi_square_job.py",
        "data/intermediate/counts",
        "data/intermediate/chi2_scores",
    )
    run_job(
        "src/jobs/topk_job.py",
        "data/intermediate/chi2_scores",
        "data/intermediate/topk_outputs",
    )

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
