Contains modified code from: https://github.com/pbour/ijoin

Example:
./ij -j anti -t 8 ./inputs/test1.tsv ./inputs/test2.tsv

FS version that is always used is sequential bguFS in a master-worker manner.

Input parameter -j provides the join type that the user wants. Available join types are: inner, left, right, full, anti
Input parameter -t provides the number of threads to be used (>=1)

Input format extended to 4 columns (2 non-temporal attributes) - sorting phase sorts relations by 1) groups and 2) start point - many bguFSs run for same groups.

Original code modified to also produce workload count.
