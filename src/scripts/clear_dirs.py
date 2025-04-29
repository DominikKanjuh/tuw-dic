import shutil
from pathlib import Path


def clear_directory(path: Path):
    if path.exists() and path.is_dir():
        for item in path.iterdir():
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)


if __name__ == "__main__":
    base = Path(__file__).resolve().parents[2] / "data"
    clear_directory(base / "intermediate")
    clear_directory(base / "output")
