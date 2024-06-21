Contains modified code from: https://github.com/pbour/ijoin

Example:
./ij -t 8 ./inputs/test1.tsv ./inputs/test2.tsv

FS version that is always used is sequential bguFS in a master-worker manner

Change the join type defined in def.h to produce the results for the join operation of your choice
Be careful to only define 1 join type

Input format extended to 4 columns (+ 2 non-temporal attributes) - sorting phase sorts relations by 1) groups and 2) start point - many FSs run for same groups

Original code modified to also produce workload count
