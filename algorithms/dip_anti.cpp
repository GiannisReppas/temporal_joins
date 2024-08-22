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

inline char overlap( Timestamp& start, Timestamp& end, Timestamp& leadStart, Timestamp& leadEnd)
{
	if ( end <= leadStart )
		return 1;

	if ( start >= leadEnd )
		return 2;

	return 0;
}

uint64_t dip_merge( std::vector<dip_heap_node>& heap_r, Relation& S, Timestamp& domainStart, Timestamp& domainEnd)
{
	uint64_t result = 0;

	// lead variables
	Timestamp longestS = domainStart;
	Timestamp leadStart, leadEnd;

	// initialize current position for pointers in heap_r
	size_t partitions_num = heap_r.size();
	size_t* currentR = (size_t*) malloc( partitions_num * sizeof(size_t) );
	for (uint32_t i = 0; i < partitions_num; i++)
		currentR[i] = 0;

	// scan S and get s.X in every step of the loop
	Record* currentS = S.record_list;
	Record* lastS = S.record_list + S.numRecords;
	char c;
	while (currentS != lastS)
	{
		// get s.X, scan partitions if its > 0
		if (longestS < currentS->start)
		{
			leadStart = longestS;
			leadEnd = currentS->start;
			longestS = currentS->end;

			// scan DIP partitions for s.X
			for (uint32_t i = 0; i < partitions_num; i++)
			{
				for ( ; currentR[i] < heap_r[i].partition.size(); currentR[i]++ )
				{
					// 3 cases: a) overlap, b) no overlap and continue c) no overlap and break;
					c = overlap( heap_r[i].partition[currentR[i]].start , heap_r[i].partition[currentR[i]].end , leadStart , leadEnd );
					if ( c == 0 )
					{
#ifdef WORKLOAD_COUNT
						result += 1;
#else
						result += heap_r[i].partition[ currentR[i] ].start ^ leadStart;
#endif
					}
					else if ( c == 2 )
					{
						if (currentR[i] > 0)
							currentR[i]--;
						break;
					}
				}
				if (c == 0)
					if (currentR[i] > 0)
						currentR[i]--;
			}
		}
		else if (longestS < currentS->end)
		{
			longestS = currentS->end;
		}

		currentS++;
	}

	// one last step for last lead (doesnt exist in original DIP but its part of complement approach)
	currentS--;
	if ( longestS < domainEnd)
	{
		leadStart = longestS;
		leadEnd = domainEnd;

		for (uint32_t i = 0; i < partitions_num; i++)
		{
			for (uint32_t j = currentR[i]; j < heap_r[i].partition.size(); j++ )
			{
				c = overlap( heap_r[i].partition[j].start , heap_r[i].partition[j].end , leadStart , leadEnd );
				if (c == 0)
				{
#ifdef WORKLOAD_COUNT
					result += 1;
#else
					result += heap_r[i].partition[ j ].start ^ leadStart;
#endif
				}
			}
		}
	}

	// return result
	free( currentR );
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