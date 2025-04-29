import subprocess


def run_job(script, input_file, output_file, extra_args=[]):
    cmd = ["python", script] + extra_args + [input_file]
    with open(output_file, "w") as out:
        subprocess.run(cmd, stdout=out, check=True)
    print(f"Completed {script}")


def main():
    run_job(
        "src/jobs/preprocess_job.py",
        "data/input/reviews_devset.json",
        "data/intermediate/preprocessed.txt",
        ["--stopwords", "data/input/stopwords.txt"],
    )
    run_job(
        "src/jobs/count_job.py",
        "data/intermediate/preprocessed.txt",
        "data/intermediate/counts.txt",
    )
    run_job(
        "src/jobs/chi_square_job.py",
        "data/intermediate/counts.txt",
        "data/intermediate/chi2_scores.txt",
    )
    run_job(
        "src/jobs/topk_job.py",
        "data/intermediate/chi2_scores.txt",
        "data/output/top75_terms.txt",
    )
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
