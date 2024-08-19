/******************************************************************************
 * Project:  temporal_joins
 * Purpose:  Compute temporal joins with conjunctive equality predicates
 * Author:   Ioannis Reppas, giannisreppas@hotmail.com
 ******************************************************************************
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

#include "getopt.h"
#include "def.hpp"
#include "containers/borders_element.hpp"
#include "containers/relation.hpp"
#include "containers/bucket_index.hpp"

// findBorders
void mainBorders( ExtendedRelation& R, Borders& bordersR, ExtendedRelation& S, Borders& bordersS, uint32_t c);

// complement
void convert_to_complement( ExtendedRelation& R, Borders& borders, ExtendedRelation& complement, Borders& borders_complement,
							Timestamp foreignStart, Timestamp foreignEnd, uint32_t c);

// bguFS
uint64_t bguFS(Relation &R, Relation &S, BucketIndex &BIR, BucketIndex &BIS);

// used to get the id of an available thread
uint32_t getThreadId(bool& needsDetach, uint32_t* jobsList, uint32_t& jobsListSize);

/* code */

/* function defining sorting of ExtendedRelation */
bool sortByGroupAndStartPoint( ExtendedRecord a, ExtendedRecord b)
{
	if (a.group1 != b.group1)
		return a.group1 < b.group1;
	else if (a.group2 != b.group2)
		return a.group2 < b.group2;
	else
		return a.start < b.start;
}

struct structForParallelFS
{
	ExtendedRelation* exR;					// relation R with non-temporal values
	ExtendedRelation* exS;					// relation S with non-temporal values
	uint32_t R_start;					// start position to run bguFS from exR
	uint32_t R_end;						// end position to run bguFS from exR
	uint32_t S_start;					// start position to run bguFS from exS
	uint32_t S_end;						// end position to run bguFS from exS

	uint32_t threadId;						// thread id
	uint64_t* thread_results;	// array that keeps the results of each thread
	uint32_t* jobsList;			// list of threads - needs to be updated at the end of the computation
};

void* worker_bguFS(void* args)
{
	structForParallelFS *gained = (structForParallelFS*) args;

	Relation R;
	R.numRecords = 0;
	R.minStart = std::numeric_limits<Timestamp>::max();
	R.maxStart = std::numeric_limits<Timestamp>::min();
	R.minEnd   = std::numeric_limits<Timestamp>::max();
	R.maxEnd   = std::numeric_limits<Timestamp>::min();
	R.load( *(gained->exR), gained->R_start, gained->R_end);

	Relation S;
	S.numRecords = 0;
	S.minStart = std::numeric_limits<Timestamp>::max();
	S.maxStart = std::numeric_limits<Timestamp>::min();
	S.minEnd   = std::numeric_limits<Timestamp>::max();
	S.maxEnd   = std::numeric_limits<Timestamp>::min();
	S.load( *(gained->exS), gained->S_start, gained->S_end);

	BucketIndex BIR, BIS;
	BIR.build(R, 1000);
	BIS.build(S, 1000);

	gained->thread_results[ gained->threadId ] += bguFS(R, S, BIR, BIS);

	// make current thread free to be used for next group
	gained->jobsList[ gained->threadId ] = 2;

	return NULL;
}

uint64_t extended_temporal_join( ExtendedRelation& exR, Borders& bordersR, ExtendedRelation& exS, Borders& bordersS,
										   uint32_t runNumThreads, bool outerFlag)
{
	#ifdef TIMES
	Timer tim;
	tim.start();
	#endif

	// variables required for master-slave thread scheduling
	uint64_t result = 0;
	uint64_t* thread_results = (uint64_t*) malloc( runNumThreads*sizeof(uint64_t) );
	uint32_t* jobsList = (uint32_t*) malloc( runNumThreads*sizeof(uint32_t) );
	for (uint32_t i = 0; i < runNumThreads; i++)
	{
		jobsList[i] = 1;
		thread_results[i] = 0;
	}
	structForParallelFS toPass[runNumThreads];
	pthread_t threads[runNumThreads];
	uint32_t threadId;
	bool needsDetach;

	// loop through Relations existing in ExtendedRelations
	Timestamp domainStart = std::min(exR.minStart, exS.minStart);
	std::vector< BordersElement >::iterator it_exR = bordersR.begin();
	std::vector< BordersElement >::iterator it_exS = bordersS.begin();
	while (it_exR != bordersR.end())
	{
		if ( (it_exS == bordersS.end()) || ((it_exR->group1 < it_exS->group1) || ((it_exR->group1 == it_exS->group1)) && (it_exR->group2 < it_exS->group2)) )
		{
			if (outerFlag)
			{
				// join between R and time_domain (= R)
#ifdef WORKLOAD_COUNT
				result += it_exR->position_end - it_exR->position_start + 1;
#else
				for (uint32_t i=it_exR->position_start; i < it_exR->position_end ; i++)
					result += domainStart ^ exR.record_list[i].start;
#endif
			}

			it_exR++;
		}
		else if ((it_exR->group1 > it_exS->group1) || ((it_exR->group1 == it_exS->group1)) && (it_exR->group2 > it_exS->group2))
		{
			it_exS++;
		}
		else
		{
			if ( (it_exS->position_start != 1) || (it_exS->position_end != 0) )
			{
				needsDetach = false;
				threadId = getThreadId(needsDetach, jobsList, runNumThreads);
				if (needsDetach)
					if (pthread_detach(threads[threadId]))
						printf("Whoops\n");

				toPass[threadId].exR = &exR;
				toPass[threadId].exS = &exS;
				toPass[threadId].R_start = it_exR->position_start;
				toPass[threadId].R_end = it_exR->position_end;
				toPass[threadId].S_start = it_exS->position_start;
				toPass[threadId].S_end = it_exS->position_end;

				toPass[threadId].threadId = threadId;
				toPass[threadId].jobsList = jobsList;
				toPass[threadId].thread_results = thread_results;
				
				pthread_create( &threads[threadId], NULL, worker_bguFS, &toPass[threadId]);
			}

			it_exR++;
			it_exS++;
		}
	}
	for (uint32_t i=0; i < runNumThreads; i++)
	{
			if (jobsList[i] != 1)
				pthread_join( threads[i], NULL);
	}

	for (uint32_t i = 0; i < runNumThreads; i++)
		result += thread_results[i];

	free( thread_results );
	free( jobsList );

	#ifdef TIMES
	double timeInnerJoin = tim.stop();
	std::cout << "Inner Join time: " << timeInnerJoin << std::endl;
	#endif

	return result;
}

int main(int argc, char **argv)
{
	uint32_t runNumThreads = 0;
	uint64_t result = 0;
	int joinType;

	// Parse and check command line input.
	if (argc != 7)
	{
		printf("Usage: ./ij -j joinType -t threadNum FILE1 FILE2\n");
		exit(1);
	}
	char c;
	while ((c = getopt(argc, argv, "j:t:")) != -1)
	{
		switch (c)
		{
			case 'j':
				if (!strcmp(optarg,"inner"))
				{
					joinType = INNER_JOIN;
				}
				else if (!strcmp(optarg,"left"))
				{
					joinType = LEFT_OUTER_JOIN;
				}
				else if (!strcmp(optarg,"right"))
				{
					joinType = RIGHT_OUTER_JOIN;
				}
				else if (!strcmp(optarg,"full"))
				{
					joinType = FULL_OUTER_JOIN;
				}
				else if (!strcmp(optarg,"anti"))
				{
					joinType = ANTI_JOIN;
				}
				else
				{
					printf("Unknown Join type provided\n");
					exit(1);
				}
				break;
			case 't':
				runNumThreads = atoi(optarg);
				if (runNumThreads <= 0)
				{
					printf("More than 0 threads required\n");
					exit(1);
				}
				break;
			default:
				printf("Usage: ./ij -j joinType -t threadNum FILE1 FILE2\n");
				exit(1);
		}
	}
	if (runNumThreads == 0)
	{
		printf("No thread num provided\n");
		exit(1);
	}
	if (joinType == -1)
	{
		printf("No join type provided\n");
		exit(1);
	}
	if (argc-optind < 2)
	{
		std::cout << "error - two input files must be specified" << std::endl;
		return 1;
	}

	// Load inputs
	// Use 2 parallel threads
	ExtendedRelation exR, exS;
	pthread_t thread_id[2];
	struct LoadRelationStructure lrs[2];
	lrs[0].rel = &exR;
	lrs[0].filename = argv[ optind ];
	lrs[1].rel = &exS;
	lrs[1].filename = argv[ optind+1 ];
	pthread_create( &thread_id[0], NULL, &ExtendedRelation::load_helper, (void*) &lrs[0]);
	pthread_create( &thread_id[1], NULL, &ExtendedRelation::load_helper, (void*) &lrs[1]);
	pthread_join( thread_id[0], NULL);
	pthread_join( thread_id[1], NULL);
	printf("Relations loaded.\n\n");

	auto totalStartTime = std::chrono::steady_clock::now();

	// sort
	#ifdef TIMES
	Timer tim;
	tim.start();
	#endif
	std::sort( &exR.record_list[0], &exR.record_list[0] + exR.numRecords, sortByGroupAndStartPoint);
	std::sort( &exS.record_list[0], &exS.record_list[0] + exS.numRecords, sortByGroupAndStartPoint);

	#ifdef TIMES
	double timeSorting = tim.stop();
	std::cout << "Sorting time: " << timeSorting << std::endl;
	#endif

	// find borders of each group
	Borders bordersR;
	Borders bordersS;
	mainBorders( exR, bordersR, exS, bordersS, runNumThreads);

	// run join
	if (joinType == INNER_JOIN)
	{
		result += extended_temporal_join( exR, bordersR, exS, bordersS, runNumThreads, false);
	}
	else if (joinType == LEFT_OUTER_JOIN)
	{
		result += extended_temporal_join( exR, bordersR, exS, bordersS, runNumThreads, false);

		ExtendedRelation exS_complement;
		Borders bordersS_complement;
		convert_to_complement( exS, bordersS, exS_complement, bordersS_complement, exR.minStart, exR.maxEnd, runNumThreads);
		result += extended_temporal_join( exR, bordersR, exS_complement, bordersS_complement, runNumThreads, true);
	}
	else if (joinType == RIGHT_OUTER_JOIN)
	{
		result += extended_temporal_join( exR, bordersR, exS, bordersS, runNumThreads, false);

		ExtendedRelation exR_complement;
		Borders bordersR_complement;
		convert_to_complement( exR, bordersR, exR_complement, bordersR_complement, exS.minStart, exS.maxEnd, runNumThreads);
		result += extended_temporal_join( exS, bordersS, exR_complement, bordersR_complement, runNumThreads, true);
	}
	else if (joinType == FULL_OUTER_JOIN)
	{
		result += extended_temporal_join( exR, bordersR, exS, bordersS, runNumThreads, false);

		ExtendedRelation exS_complement;
		Borders bordersS_complement;
		convert_to_complement( exS, bordersS, exS_complement, bordersS_complement, exR.minStart, exR.maxEnd, runNumThreads);
		result += extended_temporal_join( exR, bordersR, exS_complement, bordersS_complement, runNumThreads, true);
		
		ExtendedRelation exR_complement;
		Borders bordersR_complement;
		convert_to_complement( exR, bordersR, exR_complement, bordersR_complement, exS.minStart, exS.maxEnd, runNumThreads);
		result += extended_temporal_join( exS, bordersS, exR_complement, bordersR_complement, runNumThreads, true);
	}
	else if (joinType == ANTI_JOIN)
	{
		ExtendedRelation exS_complement;
		Borders bordersS_complement;
		convert_to_complement( exS, bordersS, exS_complement, bordersS_complement, exR.minStart, exR.maxEnd, runNumThreads);
		result += extended_temporal_join( exR, bordersR, exS_complement, bordersS_complement, runNumThreads, true);
	}

	// Report stats
	auto totalEndTime = std::chrono::steady_clock::now();

	std::cout << "\nTotal count: " << result << std::endl;
	std::cout << "Total time: " << std::chrono::duration_cast<std::chrono::milliseconds>(totalEndTime - totalStartTime).count() << " ms" << std::endl;

	return 0;
}
