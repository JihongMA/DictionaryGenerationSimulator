#!/bin/bash

python test.py --distribution zipf --delay 0 --protocol leader
cat results/results.csv > worker.txt
python test.py --distribution zipf --delay 0 --protocol bully
cat results/results.csv >> worker.txt
python test.py --distribution uniform --delay 0 --protocol leader
cat results/results.csv >> worker.txt
python test.py --distribution uniform --delay 0 --protocol bully
cat results/results.csv >> worker.txt

