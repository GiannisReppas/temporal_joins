# Parallel Processing of Temporal Anti-Joins in Memory

This repository contains the source code for the paper:

> Ioannis Reppas, Meghdad Mirabi, Leila Fathi, Carsten Binnig, Anton Dignös, Johann Gamper.
> **Parallel Processing of Temporal Anti-Joins in Memory.**
> In *Database Systems for Advanced Applications (DASFAA 2024)*, LNCS 14850, pp. 86–102. Springer, 2024.
> DOI: [10.1007/978-981-97-5552-3_6](https://doi.org/10.1007/978-981-97-5552-3_6)

It is a C++/pthreads implementation of in-memory, multi-threaded temporal joins with conjunctive equality predicates on non-temporal attributes. The main focus is the **temporal anti-join**. Inner, left outer, right outer and full outer temporal joins are also supported.

> **Branches.** `dev` (this branch) is an **optimized and extended** version of the paper's implementation:
> * group borders are stored in flat arrays, built in parallel with a count pass and a write pass, instead of ordered maps;
> * the DIP baseline supports all join types, plus an optimized anti-join variant (`oDIP`);
> * the `-n` option repeats the join phase.
>
> The code used for the experiments in the paper is on the default [`dasfaa2024`](../../tree/dasfaa2024) branch, where the group borders are stored in a `std::map`. To reproduce the paper's numbers exactly, use that branch.

---

## Idea in a nutshell

A temporal anti-join `R ▷ᵀ_{R.C = S.D} S` returns, for each tuple of `R`, the maximal sub-intervals of its timestamp during which no tuple of `S` with matching non-temporal values exists.

Instead of computing it directly, we:

1. **Compute the complement `S̄` of `S`** with respect to the join attributes `D`. For each distinct value combination of `D`, `S̄` contains the maximal time intervals (within the time domain) that are *not* covered by any tuple of `S` with those values.
2. **Rewrite the anti-join as a temporal inner join** (Lemma 1 in the paper):

   ```
   R ▷ᵀ_θ S  ≡  π_{R.A ∘ R.T ∩ S̄.T} (R ⋈ᵀ_θ S̄)  ∪  { r ∈ R | ∄ s ∈ S : r.C = s.D }
   ```

Every operator in the rewritten expression splits into independent sub-tasks, one per group of tuples with equal join values, so all steps run in parallel:

| Step | Paper | Source file |
|---|---|---|
| Sort `R` and `S` by join attributes, then start point | Alg. 1, lines 1–2 | `main.cpp` |
| Find the borders (first/last position) of each group, in parallel. Uses arrays here instead of the paper's hash tables | Alg. 2 `Borders` | `algorithms/findBorders.cpp`, `containers/borders.*` |
| Build the complement `S̄`, in parallel (count pass + write pass) | Alg. 3 `Complement`, Alg. 4 `PartialComplement` | `algorithms/complement.cpp` |
| Merge-join the group borders of `R` and `S̄`. Each matching group is handed to a free worker thread running `bguFS` | Alg. 5 `TemporalJoin` | `main.cpp` (`extended_temporal_join`), `algorithms/scheduling.cpp` |
| Forward-scan plane-sweep interval join | `bguFS` [Bouros & Mamoulis] | `algorithms/bgufs.cpp`, `containers/bucket_index.*` |
| Baseline: Disjoint Interval Partitioning, extended with equality predicates | DIP [Cafagna & Böhlen] | `algorithms/dip.cpp` |

Outer joins use the same building blocks. For example, a left outer join is `R ⋈ᵀ S` plus `R ⋈ᵀ S̄`.

---

## Repository layout

```
.
├── main.cpp                 # CLI, sorting, master–worker temporal join driver
├── def.hpp                  # global definitions and compile-time switches (TIMES, WORKLOAD_COUNT)
├── makefile
├── algorithms/
│   ├── findBorders.cpp      # parallel computation of group borders (Alg. 2)
│   ├── complement.cpp       # parallel complement of a temporal relation (Alg. 3 & 4)
│   ├── bgufs.cpp            # bguFS forward-scan interval join (modified from ijoin)
│   ├── dip.cpp              # DIP baseline: inner join, anti-join, optimized anti-join (oDIP)
│   └── scheduling.cpp       # helper that hands work to a free worker thread
├── containers/
│   ├── relation.*           # Relation / ExtendedRelation (tuples with 2 non-temporal attributes)
│   ├── borders.*            # flat array of (group1, group2, position_start, position_end)
│   └── bucket_index.*       # bucket index used by bguFS
└── inputs/                  # small toy inputs
```

---

## Building

Requirements: a C++14 compiler with pthreads and a CPU with AVX. The makefile uses `-O3 -mavx -march=native`.

```bash
make          # produces the executable ./ij
make clean
```

---

## Usage

```
./ij -j <joinType> -a <algorithm> -t <threads> [-n <repetitions>] FILE_R FILE_S
```

| Option | Values | Meaning |
|---|---|---|
| `-j` | `inner`, `left`, `right`, `full`, `anti` | Temporal join type. `anti` computes `R ▷ᵀ S`. |
| `-a` | `bguFS`, `DIP`, `oDIP` | Algorithm. `bguFS` is the proposed complement-based solution. `DIP` is the extended DIP baseline. `oDIP` is an optimized DIP variant for anti-joins only. |
| `-t` | integer ≥ 1 | Number of threads. |
| `-n` | integer ≥ 1 (optional, default 1) | Number of times the join phase is repeated (useful for timing). Loading, sorting and border detection run only once. |

All options must come before the two input files. `FILE_R` is the left (outer) relation and `FILE_S` the right one.

Examples:

```bash
# proposed solution, temporal anti-join, 8 threads
./ij -j anti -a bguFS -t 8 ./inputs/test1.tsv ./inputs/test2.tsv

# DIP baseline for the same query
./ij -j anti -a DIP -t 8 ./inputs/test1.tsv ./inputs/test2.tsv

# full outer temporal join, repeated 5 times
./ij -j full -a bguFS -t 4 -n 5 ./inputs/test1.tsv ./inputs/test2.tsv
```

Supported combinations:

| | inner | left | right | full | anti |
|---|:-:|:-:|:-:|:-:|:-:|
| `bguFS` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `DIP`   | ✓ | ✓ | ✓ | ✓ | ✓ |
| `oDIP`  |   |   |   |   | ✓ |

---

## Input format

Each input is a plain-text file with one tuple per line and four whitespace-separated unsigned integers:

```
<start> <end> <attr1> <attr2>
```

* `start`, `end`: the timestamp as a **half-open** interval `[start, end)`.
* `attr1`, `attr2`: two non-temporal attributes. The join predicate is always the conjunctive equality `R.attr1 = S.attr1 ∧ R.attr2 = S.attr2`. To join on a single attribute, set the second one to a constant (e.g. `0`) in both files.

Example (`inputs/test1.tsv`):

```
1 10 1 2
1 7 1 1
2 3 1 2
```

The time domain used for the complement is taken from the data: the minimum start and maximum end of the relevant relation.

---

## Output

To measure join processing rather than result materialization, result tuples are **not written out**. The program prints a single aggregate in `Total count` and the elapsed time:

```
Relations loaded.

Sorting time: ...
FindBorders time: ...
----------------------
Complement time: ... and size ...
Inner Join time: ...

Total count: ...
Total time: ... ms
```

What `Total count` means depends on two switches in `def.hpp`:

| Switch | Default | Effect |
|---|---|---|
| `TIMES` | on | Prints per-phase timings (sorting, borders, complement, join). |
| `WORKLOAD_COUNT` | off | When **on**, `Total count` is the number of result tuples. When **off**, it is a checksum (XOR-based sum of start points) over all result pairs. The checksum makes sure the compiler cannot optimize the join away, and it lets you compare results across algorithms and thread counts. |

Recompile (`make clean && make`) after changing these switches.

---

## Datasets used in the paper

> The paper's results were produced with the [`dasfaa2024`](../../tree/dasfaa2024) branch. This branch has been optimized since, so its runtimes will differ from the paper.

The paper uses four public real-world datasets:

| Dataset | Source |
|---|---|
| Taxis | NYC TLC trip records – <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> |
| Dig | Chicago Dig Ticket Notifications – <https://data.cityofchicago.org/Transportation/Dig-Ticket-Notifications/cygx-ui4j> |
| Divvy | Chicago Divvy Trips – <https://data.cityofchicago.org/Transportation/Divvy-Trips/fg6s-gzvg> |
| Emergency | NYC Health open data – <https://data.cityofnewyork.us/browse?category=Health> |

For each dataset, convert the records to the 4-column format above (integer start/end time points and two integer-encoded non-temporal attributes). Then randomly split the tuples into two halves, which serve as `R` and `S`. In the paper, single-threaded runs compare `-a bguFS` and `-a DIP` with `-t 1`. The speed-up experiments vary `-t` over {1, 4, 8, 12, 16, 20, 24}.

The paper's experiments ran on a 12-core Intel Xeon Silver 4214R (2.40 GHz, 24 hardware threads with hyper-threading) with 32 GB RAM.

---

## Acknowledgements

* `bguFS` and the bucket index are adapted from **ijoin** by Panagiotis Bouros and Nikos Mamoulis: <https://github.com/pbour/ijoin>
  * P. Bouros, N. Mamoulis. *A forward scan based plane sweep algorithm for parallel interval joins.* VLDB 2017.
  * P. Bouros, N. Mamoulis, D. Tsitsigkos, M. Terrovitis. *In-memory interval joins.* VLDB Journal 30, 2021.
* The DIP baseline follows F. Cafagna, M. H. Böhlen. *Disjoint interval partitioning.* VLDB Journal 26, 2017. It is extended here with equality predicates on non-temporal attributes.

This work was partially supported by hessian.AI at TU Darmstadt and DFKI Darmstadt, and by a grant from the Autonomous Province of Bozen-Bolzano "Research Südtirol/Alto Adige 2019" (project ISTeP).

---

## Citation

If you use this code, please cite:

```bibtex
@inproceedings{reppas2024antijoins,
  author    = {Reppas, Ioannis and Mirabi, Meghdad and Fathi, Leila and
               Binnig, Carsten and Dign{\"o}s, Anton and Gamper, Johann},
  title     = {Parallel Processing of Temporal Anti-Joins in Memory},
  booktitle = {Database Systems for Advanced Applications (DASFAA 2024)},
  series    = {Lecture Notes in Computer Science},
  volume    = {14850},
  pages     = {86--102},
  publisher = {Springer},
  year      = {2024},
  doi       = {10.1007/978-981-97-5552-3_6}
}
```

## License

MIT. See [LICENSE.md](LICENSE.md).
