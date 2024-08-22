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

#include "../containers/relation.hpp"

class dip_heap_node
{
public:
	Timestamp max_end_point;
	std::vector<Record> partition;

	dip_heap_node(Record r)
	{
		this->max_end_point = r.end;
		this->partition.push_back(r);
	}
};

bool heapComparison( dip_heap_node a, dip_heap_node b )
{
	return a.max_end_point < b.max_end_point;
}

void create_dip(Relation& R, std::vector<dip_heap_node>& heap_r)
{
	heap_r.push_back( dip_heap_node(R.record_list[0]) );
	for (uint32_t i = 1; i != R.numRecords; i++)
	{
		if ( heap_r.front().max_end_point > R.record_list[i].start )
		{
			heap_r.push_back( dip_heap_node(R.record_list[i]) );

			push_heap( heap_r.begin(), heap_r.end(), heapComparison);
		}
		else
		{
			heap_r[0].max_end_point = R.record_list[i].end;
			heap_r[0].partition.push_back( R.record_list[i] );

			pop_heap( heap_r.begin(), heap_r.end(), heapComparison);
			push_heap( heap_r.begin(), heap_r.end(), heapComparison);
		}
	}
}

uint64_t dip_merge( std::vector<dip_heap_node>& heap_r, Relation& S, Timestamp& domainStart, Timestamp& domainEnd)
{
	uint64_t result = 0;

	// lead variables
	Timestamp longestS = domainStart;
	Timestamp leadStart, leadEnd;

	// initialize current and end position for pointers in heap_r
	size_t partitions_num = heap_r.size();
	Record** currentR = (Record**) malloc( partitions_num * sizeof(Record*) );
	Record** endR = (Record**) malloc( partitions_num * sizeof(Record*) );
	for (uint32_t i = 0; i < partitions_num; i++)
	{
		currentR[i] = &heap_r[i].partition[0];
		endR[i] = &heap_r[i].partition[0] + heap_r[i].partition.size();
	}

	// scan S and get s.X in every step of the loop
	Record* currentS = S.record_list;
	Record* lastS = S.record_list + S.numRecords;
	while (currentS != lastS)
	{
		// get s.X, scan partitions for overlaps, if its > 0
		if (longestS < currentS->start)
		{
			leadStart = longestS;
			leadEnd = currentS->start;
			longestS = currentS->end;

			// scan DIP partitions for s.X
			for (uint32_t i = 0; i < partitions_num; i++)
			{
				while (currentR[i] != endR[i])
				{
					// 3 cases:
					// a) overlap -> output result tuple and move to next tuple from R
					// b) no overlap and move to next tuple from R (r is left of S.X)
					// c) no overlap and break (r is right of S.X);
					if ( currentR[i]->start >= leadEnd ) // case b
					{
						break;
					}
					else if ( currentR[i]->end > leadStart ) // case a
					{
#ifdef WORKLOAD_COUNT
						result += 1;
#else
						result += currentR[i]->start ^ leadStart;
#endif
					}

					currentR[i]++;
				}
				if (currentR[i] != &heap_r[i].partition[0])
					currentR[i]--;
			}
		}
		else if (longestS < currentS->end)
		{
			longestS = currentS->end;
		}

		currentS++;
	}

	// one last step for last lead with domainEnd (doesnt exist like that in original DIP but its part of complement design)
	currentS--;
	if ( longestS < domainEnd)
	{
		leadStart = longestS;
		leadEnd = domainEnd;

		for (uint32_t i = 0; i < partitions_num; i++)
		{
			while (currentR[i] != endR[i])
			{
				if ( currentR[i]->start >= leadEnd ) // case b
				{
					break;
				}
				else if ( currentR[i]->end > leadStart ) // case a
				{
#ifdef WORKLOAD_COUNT
					result += 1;
#else
					result += currentR[i]->start ^ leadStart;
#endif
				}

				currentR[i]++;
			}
		}
	}

	// return result
	free( currentR );
	free( endR );
	return result;
}

uint64_t dip_anti(Relation& R, Relation& S, Timestamp& domainStart, Timestamp& domainEnd)
{
	#ifdef TIMES
	Timer tim;
	tim.start();
	#endif

	std::vector<dip_heap_node> heap_r;
	create_dip( R, heap_r);

	#ifdef TIMES
	double timeCreateDip = tim.stop();
	std::cout << "CreateDIP time: " << timeCreateDip << std::endl;
	tim.start();
	#endif

	uint64_t result = dip_merge( heap_r, S, domainStart, domainEnd);

	#ifdef TIMES
	double timeDipMerge = tim.stop();
	std::cout << "DipMerge time: " << timeDipMerge << std::endl;
	#endif

	return result;
}