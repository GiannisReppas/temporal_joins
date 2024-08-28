Contains modified code from: https://github.com/pbour/ijoin

Example:
./ij -j anti -t 8 -a bguFS ./inputs/test1.tsv ./inputs/test2.tsv

FS version that is always used is sequential bguFS in a master-worker manner.

Input parameter -j provides the join type that the user wants. Available join types are: inner, left, right, full, anti
Input parameter -t provides the number of threads to be used (>=1)
Input parameter -a provides the algorithm to use to compute the temporal join. bguFS is the main way to do this. DIP (and oDIP, for an optimized version) is also available for anti-joins.

Input format extended to 4 columns (2 non-temporal attributes) - sorting phase sorts relations by 1) non-temporal values and 2) start point - many bguFSs run for same non-temporal values.

Original code modified to also produce workload count.
