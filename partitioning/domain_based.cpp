/******************************************************************************
 * Project:  temporal_joins
 * Purpose:  Compute temporal joins with conjunctive equality predicates
 * Author:   Ioannis Reppas, giannisreppas@hotmail.com
 ******************************************************************************
 * Copyright (c) 2017, Panagiotis Bouros
 * Copyright (c) 2023, Ioannis Reppas
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/


#include "../def.h"
#include "../containers/relation.h"
#include "../containers/bucket_index.h"

// Very simple load estimation functions but they work very well in practice.
inline size_t ComputeLoad(long int pid, vector<size_t> &curr_sR, vector<size_t> &curr_sS)
{
	return curr_sR[pid]*curr_sS[pid];
}


inline size_t ComputeIncreasedNewLoad(long int pid, vector<size_t> &curr_sR, vector<size_t> &curr_sS, vector<size_t> &shistR, vector<size_t> &shistS, long int gid)
{
	return (curr_sR[pid]+shistR[gid])*(curr_sS[pid]+shistS[gid]);
}


inline size_t ComputeDecreasedNewLoad(long int pid, vector<size_t> &curr_sR, vector<size_t> &curr_sS, vector<size_t> &shistR, vector<size_t> &shistS, long int gid)
{
	return (curr_sR[pid]-shistR[gid])*(curr_sS[pid]-shistS[gid]);
}

class mycomparison
{
public:
	bool operator() (const pair<long int, size_t>& lhs, const pair<long int, size_t>& rhs) const
	{
		return (rhs.second > lhs.second);
	}
} myobject;

inline void CountPartitionContents(const Relation& R, size_t *pR_size, size_t *prR_size, Timestamp gmin, Timestamp gmax, Timestamp partitionExtent, long int runNumPartitionsPerRelation)
{
	for (const auto& r : R)
	{
		auto i = (r.start == gmin) ? 0 : (r.start-gmin)/partitionExtent;
		auto j = (r.end == gmax) ? (runNumPartitionsPerRelation-1) : (r.end-gmin)/partitionExtent;
		
		if (r.start == gmax)
			i = runNumPartitionsPerRelation-1;
		pR_size[i]++;
		while (i != j)
		{
			i++;
			prR_size[i]++;
		}
	}
}


inline void CountPartitionContents(const Relation& R, size_t *pR_size, size_t *prR_size, size_t *prfR_size, Timestamp gmin, Timestamp gmax, Timestamp partitionExtent, long int runNumPartitionsPerRelation)
{
	for (const auto& r : R)
	{
		auto i = (r.start == gmin) ? 0 : (r.start-gmin)/partitionExtent;
		auto j = (r.end == gmax) ? (runNumPartitionsPerRelation-1) : (r.end-gmin)/partitionExtent;
		
		if (r.start == gmax)
			i = runNumPartitionsPerRelation-1;
		pR_size[i]++;
		while (i != j)
		{
			i++;
			if (i < j)
				prfR_size[i]++;
			else
				prR_size[i]++;
		}
	}
}


inline void CountPartitionContents(const Relation& R, size_t *pR_size, size_t *prR_size, vector<Timestamp> &boundaries, long int runNumPartitionsPerRelation)
{
	for (const auto& r : R)
	{
		auto i = 0;
		while ((i < runNumPartitionsPerRelation) && (boundaries[i] < r.start))
			i++;
		if (i == runNumPartitionsPerRelation)
			i = runNumPartitionsPerRelation-1;
		pR_size[i]++;
		
		i++;
		while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < r.end))
		{
			prR_size[i]++;
			i++;
		}
	}
}


inline void CountPartitionContents(const Relation& R, size_t *pR_size, size_t *prR_size, size_t *prfR_size, vector<Timestamp> &boundaries, long int runNumPartitionsPerRelation)
{
	for (const auto& r : R)
	{
		auto i = 0;
		while ((i < runNumPartitionsPerRelation) && (boundaries[i] < r.start))
			i++;
		if (i == runNumPartitionsPerRelation)
			i = runNumPartitionsPerRelation-1;
		pR_size[i]++;
		
		i++;
		while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < r.end))
		{
			if ((boundaries[i] < r.end) && (i != runNumPartitionsPerRelation-1))
				prfR_size[i]++;
			else
				prR_size[i]++;
			i++;
		}
	}
}


inline void AllocateMemoryForPartitions(Relation *pR, Relation *prR, size_t *pR_size, size_t *prR_size, long int runNumPartitionsPerRelation)
{
	for (long int i = 0; i < runNumPartitionsPerRelation; i++)
	{
		pR[i].reserve(pR_size[i]);
		prR[i].reserve(prR_size[i]);
	}
}


inline void AllocateMemoryForPartitions(Relation *pR, Relation *prR, Relation *prfR, size_t *pR_size, size_t *prR_size, size_t *prfR_size, long int runNumPartitionsPerRelation)
{
	for (long int i = 0; i < runNumPartitionsPerRelation; i++)
	{
		pR[i].reserve(pR_size[i]);
		prR[i].reserve(prR_size[i]);
		prfR[i].reserve(prfR_size[i]);
	}

}


inline void BuildStartHistograms(const Relation& R, vector<size_t> &shistR, Timestamp gmin, Timestamp gmax, Timestamp granuleExtent, long int numGranules)
{
	for (const auto& r : R)
	{
		auto i = (r.start == gmin) ? 0 : (r.start-gmin)/granuleExtent;
		auto j = (r.end == gmax) ? (numGranules-1) : (r.end-gmin)/granuleExtent;

		if (r.start == gmax)
			i = numGranules-1;

		shistR[i]++;
	}
}


inline void AdaptivePartitioning(vector<size_t> &shistR, vector<size_t> &shistS, vector<Timestamp> &boundaries, Timestamp gmin, Timestamp gmax, Timestamp granuleExtent, long int numGranules, long int runNumPartitionsPerRelation)
{
	vector<long int> sbboundaries(runNumPartitionsPerRelation, 0);
	vector<size_t> curr(runNumPartitionsPerRelation, 0);
	vector<size_t> curr_sR(runNumPartitionsPerRelation, 0), curr_sS(runNumPartitionsPerRelation, 0);
	long int curr_part = 0, k = 0;
	long int numGranuelsPerPartition = numGranules/runNumPartitionsPerRelation;
	auto prev_sp = -1;
	auto checked = 0, updated = 0, prev_updated = 0;


	// Compute initial load per partition.
	for (long int p = 0; p < runNumPartitionsPerRelation; p++)
	{
		for (long int g = 0; g < numGranuelsPerPartition; g++)
		{
			auto k = p*numGranuelsPerPartition + g;
			curr_sR[p] += shistR[k];
			curr_sS[p] += shistS[k];
		}
		boundaries[p] = ((p+1)*numGranuelsPerPartition)*granuleExtent;
		sbboundaries[p] = p*numGranuelsPerPartition;
		curr[p] = ComputeLoad(p, curr_sR, curr_sS);
	}
	boundaries[runNumPartitionsPerRelation-1] = gmax;
	
	vector<pair<long int, size_t> > Q;
	auto idx = -1;
	while (checked-updated < runNumPartitionsPerRelation)
	{
		long int sp = 0, cp = 0;
		if (idx == -1)
		{
			Q.clear();
			for (long int p = 0; p < runNumPartitionsPerRelation; p++)
				Q.push_back(make_pair(p, curr[p]));
			sort(Q.begin(), Q.end(), myobject);
			idx = runNumPartitionsPerRelation-1;
		}
		sp = Q[idx].first;
		idx--;
		checked++;
		
		// Move load to the previous partition.
		if (sp == runNumPartitionsPerRelation-1 || sp != 0 && curr[sp-1] < curr[sp+1])
		{
			auto gid = sbboundaries[sp];
			auto newSP = ComputeDecreasedNewLoad(sp, curr_sR, curr_sS, shistR, shistS, gid);
			auto newSP1 = ComputeIncreasedNewLoad((sp-1), curr_sR, curr_sS, shistR, shistS, gid);
			
			while ((gid != numGranules-1) && (newSP > newSP1))
			{
				curr_sR[sp] -= shistR[gid];
				curr_sS[sp] -= shistS[gid];
				curr[sp] = newSP;
				curr_sR[sp-1] += shistR[gid];
				curr_sS[sp-1] += shistS[gid];
				curr[sp-1] = newSP1;
				boundaries[sp-1] += granuleExtent;
				sbboundaries[sp]++;
				
				gid++;
				newSP = ComputeDecreasedNewLoad(sp, curr_sR, curr_sS, shistR, shistS, gid);
				newSP1 = ComputeIncreasedNewLoad((sp-1), curr_sR, curr_sS, shistR, shistS, gid);
				updated = checked;
			}
		}
		// Move load to the next partition.
		else if (sp == 0 || curr[sp-1] > curr[sp+1])
		{
			auto gid = sbboundaries[sp+1]-1;
			auto newSP = ComputeDecreasedNewLoad(sp, curr_sR, curr_sS, shistR, shistS, gid);
			auto newSP1 = ComputeIncreasedNewLoad((sp+1), curr_sR, curr_sS, shistR, shistS, gid);
			
			while ((gid != -1) && (newSP > newSP1))
			{
				curr_sR[sp] -= shistR[gid];
				curr_sS[sp] -= shistS[gid];
				curr[sp] = newSP;
				curr_sR[sp+1] += shistR[gid];
				curr_sS[sp+1] += shistS[gid];
				curr[sp+1] = newSP1;
				boundaries[sp] -= granuleExtent;
				sbboundaries[sp+1]--;
				
				gid--;
				newSP = ComputeDecreasedNewLoad(sp, curr_sR, curr_sS, shistR, shistS, gid);
				newSP1 = ComputeIncreasedNewLoad((sp+1), curr_sR, curr_sS, shistR, shistS, gid);
				updated = checked;
			}
		}
	}
}


/////////////////////////
// With BucketIndexing //
/////////////////////////

// For the mj+atomic/adaptive or the mj+greedy/adaptive setup.
void ParallelDomainBased_Partition_MiniJoinsBreakdown_Adaptive(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, long int runNumBuckets, long int runNumPartitionsPerRelation, long int runNumThreads)
{
	long int numGranules = ((runNumPartitionsPerRelation < 10)? runNumPartitionsPerRelation*1000: runNumPartitionsPerRelation*100);
	Timestamp gmin = min(R.minStart, S.minStart), gmax = max(R.maxEnd, S.maxEnd);
	auto granuleExtent = (Timestamp)ceil((double)(gmax-gmin)/numGranules);
	vector<size_t> shistR(numGranules, 0), shistS(numGranules, 0);
	vector<Timestamp> boundaries(runNumPartitionsPerRelation, 0);
	size_t *pR_size, *pS_size, *prR_size, *prS_size, *prfR_size, *prfS_size;
	
	
	// Initiliaze auxliary counter
	pR_size = new size_t[runNumPartitionsPerRelation];
	pS_size = new size_t[runNumPartitionsPerRelation];
	prR_size = new size_t[runNumPartitionsPerRelation];
	prS_size = new size_t[runNumPartitionsPerRelation];
	prfR_size = new size_t[runNumPartitionsPerRelation];
	prfS_size = new size_t[runNumPartitionsPerRelation];
	memset(pR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
	memset(pS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
	memset(prR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
	memset(prS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
	memset(prfR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
	memset(prfS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
	
	// Partition relations.
	#pragma omp parallel num_threads(runNumThreads)
	{
		// Step 1: build histogram statistics for granules; i.e., count the records starting inside each granule
		// Use two parallel threads; one for each input relation.
		#pragma omp sections
		{
			#pragma omp section
			{
				BuildStartHistograms(R, shistR, gmin, gmax, granuleExtent, numGranules);
			}
			
			#pragma omp section
			{
				BuildStartHistograms(S, shistS, gmin, gmax, granuleExtent, numGranules);
			}
		}
		#pragma omp barrier
		
		
		// Step 2: reposition the boundaries between partitions; initial partitioning includes the same number of granules inside each partition.
		// Use one thread.
		#pragma omp single
		{
			AdaptivePartitioning(shistR, shistS, boundaries, gmin, gmax, granuleExtent, numGranules, runNumPartitionsPerRelation);
		}
		
		
		// Step 3: create and fill structures for each partition.
		// Use two parallel threads, one for each input relation.
		#pragma omp sections
		{
			#pragma omp section
			{
				// Count first the contents of each partition.
				CountPartitionContents(R, pR_size, prR_size, prfR_size, boundaries, runNumPartitionsPerRelation);
				
				// Allocate necessary memory.
				AllocateMemoryForPartitions(pR, prR, prfR, pR_size, prR_size, prfR_size, runNumPartitionsPerRelation);
				
				// Fill structures on each partition.
				for (const auto& r : R)
				{
					auto i = 0;
					while ((i < runNumPartitionsPerRelation) && (boundaries[i] < r.start))
						i++;
					if (i == runNumPartitionsPerRelation)
						i = runNumPartitionsPerRelation-1;
					pR[i].push_back(r);
					pR[i].minStart = min(pR[i].minStart, r.start);
					pR[i].maxStart = max(pR[i].maxStart, r.start);
					pR[i].minEnd   = min(pR[i].minEnd  , r.end);
					pR[i].maxEnd   = max(pR[i].maxEnd  , r.end);

					i++;
					while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < r.end))
					{
						if ((boundaries[i] < r.end) && (i != runNumPartitionsPerRelation-1))
							prfR[i].push_back(r);
						else
							prR[i].push_back(r);
						i++;
					}
				}
			}
			
			#pragma omp section
			{
				// Count first the contents of each partition.
				CountPartitionContents(S, pS_size, prS_size, prfS_size, boundaries, runNumPartitionsPerRelation);
				
				// Allocate necessary memory.
				AllocateMemoryForPartitions(pS, prS, prfS, pS_size, prS_size, prfS_size, runNumPartitionsPerRelation);
				
				// Fill structures on each partition.
				for (const auto& s : S)
				{
					auto i = 0;
					while ((i < runNumPartitionsPerRelation) && (boundaries[i] < s.start))
						i++;
					if (i == runNumPartitionsPerRelation)
						i = runNumPartitionsPerRelation-1;
					pS[i].push_back(s);
					pS[i].minStart = min(pS[i].minStart, s.start);
					pS[i].maxStart = max(pS[i].maxStart, s.start);
					pS[i].minEnd   = min(pS[i].minEnd  , s.end);
					pS[i].maxEnd   = max(pS[i].maxEnd  , s.end);

					i++;
					while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < s.end))
					{
						if ((boundaries[i] < s.end) && (i != runNumPartitionsPerRelation-1))
							prfS[i].push_back(s);
						else
							prS[i].push_back(s);
						i++;
					}
				}
			}
		}
		#pragma omp barrier
		
		
		// Step 4: sort partitions; in practice we only to sort structures that contain exclusively original records, i.e., replicas.
		// Use all available threads.
		#pragma omp for collapse(2) schedule(dynamic)
		for (long int j = 0; j < 2; j++)
		{
			for (long int i = 0; i < runNumPartitionsPerRelation; i++)
			{
				switch (j)
				{
					case 0:
						sort(pR[i].begin(), pR[i].end());
						break;
						
					case 1:
						sort(pS[i].begin(), pS[i].end());
						break;
				}
			}
		}
		#pragma omp barrier

		
		// Step 5: build bucket indices; in practice, we only to index original records, i.e., no replicas.
		// Use all available threads.
		#pragma omp for collapse(2) schedule(dynamic)
		for (long int j = 0; j < 2; j++)
		{
			for (long int i = 0; i < runNumPartitionsPerRelation; i++)
			{
				switch (j)
				{
					case 0:
						pBIR[i].build(pR[i], runNumBuckets);
						break;
						
					case 1:
						pBIS[i].build(pS[i], runNumBuckets);
						break;
				}
			}
		}
	}
	
	// Free allocated memory.
	delete[] pR_size;
	delete[] pS_size;
	delete[] prR_size;
	delete[] prS_size;
	delete[] prfR_size;
	delete[] prfS_size;
}
