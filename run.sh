#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

set -exo pipefail

pip install -r requirements.txt

user_id="$1"

if [ -z "$user_id" ]; then
    echo "User ID not provided. Please provide your user ID as the first argument."
    exit 1
fi

hadoop fs -mkdir -p hdfs:///user/"$user_id"/input
hadoop fs -put ~/dic/assignment-1/data/input/stopwords.txt hdfs:///user/"$user_id"/input/ || true

#INPUT_DATASET="hdfs:///user/dic25_shared/amazon-reviews/full/reviewscombined.json"
INPUT_DATASET="hdfs:///user/dic25_shared/amazon-reviews/full/reviews_devset.json"

python src/run_pipeline.py --cluster \
    --input "$INPUT_DATASET" \
    --stopwords hdfs:///user/"$user_id"/input/stopwords.txt \
    --output hdfs:///user/"$user_id"

echo "Pipeline completed successfully."
ls -lah output.txt
