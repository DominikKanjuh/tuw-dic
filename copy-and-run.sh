#!/usr/bin/env bash

clear

printf "Enter your user ID (e.g., e12345678): "
read -r user_id

if [ -z "$user_id" ]; then
    echo "Invalid user ID. Please provide your user ID."
    exit 1
fi

set -exo pipefail

ssh "$user_id"@lbd.tuwien.ac.at 'rm -rf ~/dic && mkdir -p ~/dic/assignment-1'

scp -r src/ run.sh data/input/stopwords.txt requirements.txt requirements.lock.txt "$user_id"@lbd.tuwien.ac.at:~/dic/assignment-1/

ssh "$user_id"@lbd.tuwien.ac.at 'mkdir -p ~/dic/assignment-1/data/input && mv ~/dic/assignment-1/stopwords.txt ~/dic/assignment-1/data/input/stopwords.txt'

ssh "$user_id"@lbd.tuwien.ac.at "set -x; ls -allah ~/dic/assignment-1 && cd ~/dic/assignment-1 && bash run.sh \"$user_id\""

echo "Cluster run completed. Copying output file..."

mkdir -p ./data/output/
scp "$user_id"@lbd.tuwien.ac.at:~/dic/assignment-1/output.txt ./data/output/output.txt

echo "Output file copied to local directory: data/output/output.txt"
