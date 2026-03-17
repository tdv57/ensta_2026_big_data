#!/bin/bash

source venv/bin/activate
port=4041
n_url=800000
pas=100000
python_pas=1000
for i in $(seq 0 $pas $n_url); do
    start=$i
    end=$((i + $pas))
    echo "Traitement de $start à $end sur le port $port"
    python3 write_wat_parquet_files.py $start $end $python_pas $port
done