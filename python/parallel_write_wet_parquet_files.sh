#!/bin/bash

source venv/bin/activate
port=4040
n_url=800000
pas=100000
for i in $(seq 0 $pas $n_url); do
    start=$i
    end=$((i + $pas))
    echo "Traitement de $start à $end sur le port $port"
    python3 write_wet_parquet_files.py $start $end $port
done