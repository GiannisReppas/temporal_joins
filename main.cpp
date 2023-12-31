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


#include "def.h"
#include "getopt.h"
#include "./containers/relation.h"
#include "./containers/bucket_index.h"



// Single-threaded processing
unsigned long long ForwardScanBased_PlaneSweep                   (Relation &R, Relation &S, bool runUnrolled);
unsigned long long ForwardScanBased_PlaneSweep_Grouping          (Relation &R, Relation &S, bool runUnrolled);
unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing(Relation &R, Relation &S, const BucketIndex &BIR, const BucketIndex &BIS, bool runUnrolled);

// Hash-based parallel processing
void ParallelHashBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, long int runNumPartitionsPerRelation, long int runNumThreads);
void ParallelHashBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, long int runNumBuckets, long int runNumPartitionsPerRelation, long int runNumThreads);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep                   (Relation *pR, Relation *pS, long int runNumPartitionsPerRelation, long int runNumThreads, bool runUnrolled);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping          (Relation *pR, Relation *pS, long int runNumPartitionsPerRelation, long int runNumThreads, bool runUnrolled);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing(Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, long int runNumPartitionsPerRelation, long int runNumThreads, bool runUnrolled);

// Domain-based parallel processing
void ParallelDomainBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, long int runNumPartitionsPerRelation, long int runNumThreads, bool runMiniJoinsBreakdown, bool runAdaptivePartitioning);
void ParallelDomainBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *BIR, BucketIndex *BIS, long int runNumPartitionsPerRelation, long int runNumBuckets, long int runNumThreads, bool runMiniJoinsBreakdown, bool runAdaptivePartitioning);
unsigned long long ParallelDomainBased_ForwardScanBased_PlaneSweep(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, long int runNumPartitionsPerRelation, long int runNumThreads, bool runUnrolled, bool runGreedyScheduling, bool runMiniJoinsBreakDown, bool runAdaptivePartitioning);
unsigned long long ParallelDomainBased_ForwardScanBased_PlaneSweep_Grouping(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, long int runNumPartitionsPerRelation, long int runNumThreads, bool runUnrolled, bool runGreedyScheduling, bool runMiniJoinsBreakdown, bool runAdaptivePartitioning);
unsigned long long ParallelDomainBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, long int runNumPartitionsPerRelation, long int runNumThreads, bool runUnrolled, bool runGreedyScheduling, bool runMiniJoinsBreakDown, bool runAdaptivePartitioning);


void toLowercase(char *buf)
{
	auto i = 0;
	while (buf[i])
	{
		buf[i] = tolower(buf[i]);
		i++;
	}
}

vector<unsigned long long> thread_results;
vector<int> bgFS_jobs;

void* traditional_bgFS(void* args)
{
	structForParallel_bgFS *gained = (structForParallel_bgFS*) args;

	Relation R;
	R.numRecords = 0;
	R.minStart = numeric_limits<Timestamp>::max();
	R.maxStart = numeric_limits<Timestamp>::min();
	R.minEnd   = numeric_limits<Timestamp>::max();
	R.maxEnd   = numeric_limits<Timestamp>::min();
	R.longestRecord = numeric_limits<Timestamp>::min();
	R.load( *(gained->exR), gained->R_start, gained->R_end);

	Relation S;
	S.numRecords = 0;
	S.minStart = numeric_limits<Timestamp>::max();
	S.maxStart = numeric_limits<Timestamp>::min();
	S.minEnd   = numeric_limits<Timestamp>::max();
	S.maxEnd   = numeric_limits<Timestamp>::min();
	S.longestRecord = numeric_limits<Timestamp>::min();
	S.load( *(gained->exS), gained->S_start, gained->S_end);

	BucketIndex BIR, BIS;
	BIR.build(R, gained->runNumBuckets);
	BIS.build(S, gained->runNumBuckets);

	thread_results[ gained->threadId ] += ForwardScanBased_PlaneSweep_Grouping_Bucketing(R, S, BIR, BIS, true);

	// make current thread free to be used for next group
	bgFS_jobs[ gained->threadId ] = 2;

	return NULL;
}

long int getThreadId(bool& needsDetach)
{
    uint32_t i=0;
    while(true)
    {
        if (bgFS_jobs[i] != 0)
        {
            if (bgFS_jobs[i] == 2)
                needsDetach = true;
            bgFS_jobs[i] = 0;
            break;
        }

        i == (bgFS_jobs.size() - 1) ? i = 0: i++;
    }

    return i;
}

unsigned long long extended_bgFS( ExtendedRelation& exR, ExtendedRelation exS, unsigned long int runNumBuckets, unsigned long int runNumThreads, bool complement)
{
	#ifdef TIMES
	Timer tim;
	#endif

	fill( thread_results.begin(), thread_results.end(), 0);
	unsigned long long result = 0;

	structForParallel_bgFS toPass[runNumThreads];
	pthread_t threads[runNumThreads];
	uint32_t threadId;
	bool needsDetach;
	fill( bgFS_jobs.begin(), bgFS_jobs.end(), 1);

	// sort, create complement
	if (complement)
	{
		#ifdef TIMES
		tim.start();
		#endif

		exS.complement(runNumThreads, exR.minStart, exR.maxEnd);

		#ifdef TIMES
		double timeComplement = tim.stop();
		cout << "Complement time: " << timeComplement << " and size " << exS.size() << endl;
		#endif
	}

	#ifdef TIMES
	tim.start();
	#endif

	// loop through Relations existing in ExtendedRelations
	map< pair<uint32_t,uint32_t>, pair<uint32_t, uint32_t> >::iterator it_exR = exR.borders.begin();
	map< pair<uint32_t,uint32_t>, pair<uint32_t, uint32_t> >::iterator it_exS = exS.borders.begin();

	while (it_exR != exR.borders.end())
	{
		if ( (it_exS == exS.borders.end()) || (it_exR->first < it_exS->first) )
		{
			if (complement)
			{
				// join between R and time_domain (= R)
				result += it_exR->second.second - it_exR->second.first + 1;
			}

			it_exR++;
		}
		else if ( it_exR->first > it_exS->first)
		{
			it_exS++;
		}
		else
		{
			if ( (it_exS->second.first != 1) || (it_exS->second.second != 0) )
			{
				needsDetach = false;
				threadId = getThreadId(needsDetach);
				if (needsDetach)
					if (pthread_detach(threads[threadId]))
						printf("Whoops\n");

				toPass[threadId].threadId = threadId;
				toPass[threadId].runNumBuckets = runNumBuckets;
				toPass[threadId].exR = &exR;
				toPass[threadId].exS = &exS;
				toPass[threadId].R_start = it_exR->second.first;
				toPass[threadId].R_end = it_exR->second.second;
				toPass[threadId].S_start = it_exS->second.first;
				toPass[threadId].S_end = it_exS->second.second;
				pthread_create( &threads[threadId], NULL, traditional_bgFS, &toPass[threadId]);
			}

			it_exR++;
			it_exS++;
		}
	}
	for (uint32_t i=0; i < runNumThreads; i++)
	{
        	if (bgFS_jobs[i] != 1)
			pthread_join( threads[i], NULL);
	}

	#ifdef TIMES
        double timeInnerJoin = tim.stop();
        cout << "Inner Join time: " << timeInnerJoin << endl;
        #endif

	for (auto& r : thread_results)
		result += r;

	return result;
}

int main(int argc, char **argv)
{
	char c;
	unsigned long int runNumBuckets = 1000, runNumThreads = 1;
	bool runUnrolled = false;
	unsigned long long result = 0;

	// Parse command line input.
	while ((c = getopt(argc, argv, "a:sb:hdt:ugmv?")) != -1)
	{
		switch (c)
		{
			case 'b':
				runNumBuckets = atoi(optarg);
				break;
			case 't':
				runNumThreads = atoi(optarg);
				break;
			case '?':
				exit(1);
			default:
				exit(1);
		}
	}

	// Sanity check
	if (runNumBuckets < 1)
	{
		cerr << "error - number of buckets must be at least 1" << endl;
		return 1;
	}
	if (argc-optind < 2)
	{
		cerr << "error - two input files must be specified" << endl;
		return 1;
	}

	// Load inputs
	ExtendedRelation exR, exS;
	#pragma omp parallel sections
	{
		#pragma omp section
		{
			exR.load(argv[optind]);
		}
		#pragma omp section
		{
			exS.load(argv[optind+1]);
		}
	}

	// sort and find borders of each group
	auto totalStartTime = chrono::steady_clock::now();
	groupSortAndBorders( exR, exS, runNumThreads);
	thread_results.resize( runNumThreads);
	bgFS_jobs.resize( runNumThreads);

	// run join
	#if defined(INNER_JOIN)
	result += extended_bgFS( exR, exS, runNumBuckets, runNumThreads, false);

	#elif defined(LEFT_OUTER_JOIN)
	result += extended_bgFS( exR, exS, runNumBuckets, runNumThreads, false);
	result += extended_bgFS( exR, exS, runNumBuckets, runNumThreads, true);

	#elif defined(RIGHT_OUTER_JOIN)
	result += extended_bgFS( exR, exS, runNumBuckets, runNumThreads, false);
	result += extended_bgFS( exS, exR, runNumBuckets, runNumThreads, true);

	#elif defined(FULL_OUTER_JOIN)
	result += extended_bgFS( exR, exS, runNumBuckets, runNumThreads, false);
	result += extended_bgFS( exR, exS, runNumBuckets, runNumThreads, true);
	result += extended_bgFS( exS, exR, runNumBuckets, runNumThreads, true);

	#elif defined(ANTI_JOIN)
	result += extended_bgFS( exR, exS, runNumBuckets, runNumThreads, true);

	#endif

	// Report stats
	auto totalEndTime = chrono::steady_clock::now();

	cout << "Total count: " << result << endl;
	cout << "Total time: " << chrono::duration_cast<chrono::milliseconds>(totalEndTime - totalStartTime).count() << " ms" << endl;
	cout << endl;

	return 0;
}
