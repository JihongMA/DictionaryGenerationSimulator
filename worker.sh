#!/bin/bash

python test.py --protocol bully --distribution uniform --lookahead 0 --order False
cat results/results.csv > worker.txt
python test.py --protocol leader --distribution uniform  --lookahead 0 --order False
cat results/results.csv >> worker.txt
python test.py --protocol bully --distribution uniform  --lookahead 0.1 --order False
cat results/results.csv >> worker.txt
python test.py --protocol leader --distribution uniform  --lookahead 0.1 --order False
cat results/results.csv >> worker.txt
python test.py --protocol bully --distribution uniform  --lookahead 1 --order True
cat results/results.csv >> worker.txt
python test.py --protocol leader --distribution uniform  --lookahead 1 --order True
cat results/results.csv >> worker.txt
python test.py --protocol local --distribution uniform  --lookahead 0 --order False
cat results/results.csv >> worker.txt
