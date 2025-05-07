import shutil
import subprocess
from pathlib import Path


def clear_directory(path: Path):
    if path.exists() and path.is_dir():
        for item in path.iterdir():
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)


def clear_hdfs_directories():
    hdfs_dirs = [
        "hdfs:///user/e12439367/data/intermediate/preprocessed",
        "hdfs:///user/e12439367/data/intermediate/counts",
        "hdfs:///user/e12439367/data/intermediate/chi2_scores",
        "hdfs:///user/e12439367/data/intermediate/topk_outputs",
    ]

    for path in hdfs_dirs:
        print(f"Removing HDFS directory: {path}")
        try:
            subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", path], check=True)
        except subprocess.CalledProcessError:
            print(f"Warning: Failed to remove HDFS directory {path}")


if __name__ == "__main__":
    base = Path(__file__).resolve().parents[2] / "data"
    clear_directory(base / "intermediate")
    clear_directory(base / "output")
    clear_hdfs_directories()
