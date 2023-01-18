#!/usr/bin/env sh

echo "Downloading example data (iris.parquet and chinook.db) to ./data ..."

mkdir -p data
cd data

curl -s -o iris.parquet "https://csvbase.com/calpaterson/iris.parquet"
curl -s -o chinookdb.zip "https://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"
unzip -o chinookdb.zip && rm chinookdb.zip


