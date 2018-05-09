#!/bin/bash

python test.py --protocol bully --distribution zipf --lookahead 0 --order False
cat results/results.csv > worker.txt
python test.py --protocol leader --distribution zipf  --lookahead 0 --order False
cat results/results.csv >> worker.txt
python test.py --protocol bully --distribution zipf  --lookahead 0.1 --order False
cat results/results.csv >> worker.txt
python test.py --protocol leader --distribution zipf  --lookahead 0.1 --order False
cat results/results.csv >> worker.txt
python test.py --protocol bully --distribution zipf  --lookahead 1 --order True
cat results/results.csv >> worker.txt
python test.py --protocol leader --distribution zipf  --lookahead 1 --order True
cat results/results.csv >> worker.txt
python test.py --protocol local --distribution zipf  --lookahead 0 --order False
cat results/results.csv >> worker.txt
