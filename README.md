# Data-intensive Computing - Chi-square Term Selection with MapReduce

This repository contains the project for the **Data-intensive Computing (194.048)** course at **TU Wien** during the **Summer 2025 semester**.

The project implements a **MapReduce pipeline** using Python's **`mrjob`** library to compute **chi-square statistics** for unigram terms in the [Amazon Review Dataset (2014)](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon/links.html), facilitating term selection for text classification.

## Team Members (Group 57)

- Amina Kadić, 12439016
- Meliha Kasapović, 12439367
- Dominik Kanjuh, 12433751
- Filip Bürgler, 12433750
- Lazar Milanov, 12329603

---

## Project Structure

```
.
├── README.md
├── data/
│   ├── input/            # Raw input files (e.g., reviews.json, stopwords.txt)
│   ├── intermediate/     # Intermediate outputs between jobs
│   └── output/           # Final output (top-k terms)
├── report/               # Project report and related documents
├── requirements.txt      # Main Python dependencies
├── requirements.lock.txt # Locked dependency versions for reproducibility
└── src/
    ├── jobs/             # MRJob scripts (one per MapReduce job)
    └── run_pipeline.py   # Pipeline orchestrator script
```

---

## Setup Instructions

### Prerequisites

- **Python 3.9.20** (matches TU Wien Hadoop cluster)
- **pip** (Python package installer)
- **[uv](https://github.com/astral-sh/uv)** for environment management (install via `pip install uv` or `brew install uv`)

### Environment Setup

1. **Create a virtual environment**:

```bash
uv venv --python 3.9.20 .venv
```

2. **Activate the environment**:

```bash
source .venv/bin/activate
```

3. **Install dependencies**:

```bash
uv pip install -r requirements.txt
```

4. **(Optional)**: Install exact versions from lockfile:

```bash
uv pip install -r requirements.lock.txt
```

---

## Running the Pipeline

From the **project root**, execute:

```bash
python src/run_pipeline.py
```

> **Note**: Use `python3` if `python` points to Python 2.x on your system.

This runs the **MapReduce pipeline** in sequence:

1. **Preprocessing**: tokenization, stopword removal
2. **Counting terms**: per category
3. **Chi-square calculation**: term-category association
4. **Top-K term selection & output formatting**

- **Intermediate outputs**: `data/intermediate/`
- **Final output**: `data/output/output.txt`

---

## Dataset

We use the **[Amazon Review Dataset (2014)](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon/links.html)**, which contains product reviews and metadata.

Place the following files in `data/input/`:

- `reviews.json` (Amazon review data)
- `stopwords.txt` (provided stopword list)

---

## License

This project is for educational purposes as part of **TU Wien’s Data-intensive Computing course (194.048)**.
