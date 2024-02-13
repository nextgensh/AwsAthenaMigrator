#!/usr/bin/env sh

directory=$1;
dbname=$2;
cd $directory;
files=`ls -1`;

for file in $files; do
    aws glue create-table --database-name "${dbname}" --table-input file://"${file}"
done
