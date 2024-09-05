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

uint64_t o_dip_merge_anti( std::vector<dip_heap_node>& heap_r, Relation& S, Timestamp& domainStart, Timestamp& domainEnd)
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

uint64_t o_dip_anti(Relation& R, Relation& S, Timestamp& domainStart, Timestamp& domainEnd)
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

	uint64_t result = o_dip_merge_anti( heap_r, S, domainStart, domainEnd);

	#ifdef TIMES
	double timeDipMerge = tim.stop();
	std::cout << "DipMerge time: " << timeDipMerge << std::endl;
	#endif

	return result;
}

uint64_t dip_merge_anti( std::vector<dip_heap_node>& heap_r, Relation& S, Timestamp& domainStart, Timestamp& domainEnd)
{
	// DIPmerge variables and result (We consider that null timepoint is +INFINITY)
	uint64_t result = 0;
	const Timestamp null_timepoint = (0 - 1);
	const uint32_t m = heap_r.size();

	// load r
	Record** current_r = (Record**) malloc( m*sizeof(Record*) );
	Record** end_r = (Record**) malloc( m*sizeof(Record*) );
	Record* r = (Record*) malloc( m*sizeof(Record) );
	bool* r_nulls = (bool*) malloc( m*sizeof(bool) );
	for (uint32_t i = 0; i < m; i++)
	{
		current_r[i] = &heap_r[i].partition[0];
		end_r[i] = &heap_r[i].partition[0] + heap_r[i].partition.size();
		r_nulls[i] = false;
		// fetchRow(R_i)
		r[i] = *(current_r[i])++;
	}

	// load s
	// DIP considers domainStart = -INFINITY, so we need some extra cleaning before main loop
	Record* current_s = S.record_list;
	const Record* end_s = S.record_list + S.numRecords;
	std::pair<Record,Record> s;
	bool s_null = false;
	// fetchRow(S)
	s.first = *current_s++;
	s.second = Record( domainStart, s.first.start );
	Timestamp longestS = domainStart;
	while (s.second.start >= s.second.end)
	{
		// fetchRow(S)
		if (current_s == end_s)
			s_null = true;
		else
			s.first = *current_s++;;
		longestS = std::max( (current_s-2)->end, longestS);
		if (s_null)
		{
			longestS = std::max( (current_s-1)->end, longestS);
			s.first.start = null_timepoint;
			if (longestS == domainEnd)
				return 0;
			else
				s.second = Record( longestS, domainEnd);
			break;
		}
		else
		{
			s.second = Record( longestS, (current_s-1)->start);
		}
	}

	// main loop
	Timestamp i = 0;
	while ( (r[i].start != null_timepoint) || (s.first.start != null_timepoint) )
	{
		if ( (s.second.start < s.second.end) && ( (r[i].start < s.second.end) && (s.second.start < r[i].end) ) ) // overlap check
				result++;

		if ( (r[i].start != null_timepoint) && ( (s.first.start == null_timepoint) || (r[i].end <= s.first.end) ) )
		{
			// fetchRow(R)
			if (current_r[i] == end_r[i])
				r_nulls[i] = true;
			else
				r[i] = *(current_r[i])++;

			if (r_nulls[i])
				r[i].start = null_timepoint;
		}
		else
		{
			if (i < (m-1))
			{
				i++;
			}
			else
			{
				i = 0;

				if (s.first.end > s.second.start)
					longestS = s.first.end;

				// fetchRow(S)
				if (current_s == end_s)
					s_null = true;
				else
					s.first = *current_s++;

				if (!s_null)
				{
					s.second = Record(longestS, s.first.start);
				}
				else
				{
					s.first.start = null_timepoint;
					s.second = Record(longestS, domainEnd);
				}
			}
		}
	}

	// last step for last lead
	for (Timestamp i = 0; i < m; i++)
	{
		if (r[i].start != null_timepoint)
		{
			for (Record* j = current_r[i]-1; j != end_r[i]; j++)
			{
				if (s.second.start < s.second.end)
					if ( (j->start < s.second.end) && (s.second.start < j->end) && (r[i].start != null_timepoint) && (s.second.end != null_timepoint) )
						result++;
			}
		}
	}

	// free and return result
	free(r_nulls);
	free(r);
	free(current_r);
	free(end_r);

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

	uint64_t result = dip_merge_anti( heap_r, S, domainStart, domainEnd);

	#ifdef TIMES
	double timeDipMerge = tim.stop();
	std::cout << "DipMerge time: " << timeDipMerge << std::endl;
	#endif

	return result;
}

uint64_t dip_merge_inner(std::vector<dip_heap_node>& heap_r, std::vector<Record>& S, Timestamp& domainStart, Timestamp& domainEnd)
{
	// DIPmerge variables and result (We consider that null timepoint is +INFINITY)
	uint64_t result = 0;
	const Timestamp null_timepoint = (0 - 1);
	const uint32_t m = heap_r.size();

	// load r
	Record** current_r = (Record**) malloc( m*sizeof(Record*) );
	Record** end_r = (Record**) malloc( m*sizeof(Record*) );
	Record* r = (Record*) malloc( m*sizeof(Record) );
	bool* r_nulls = (bool*) malloc( m*sizeof(bool) );
	for (uint32_t i = 0; i < m; i++)
	{
		current_r[i] = &heap_r[i].partition[0];
		end_r[i] = &heap_r[i].partition[0] + heap_r[i].partition.size();
		r_nulls[i] = false;
		// fetchRow(R_i)
		r[i] = *(current_r[i])++;
	}

	// load s
	// DIP considers domainStart = -INFINITY, so we need some extra cleaning before main loop
	Record* current_s = &S[0];
	const Record* end_s = &S[0] + S.size();
	Record s;
	bool s_null = false;
	// fetchRow(S)
	s = *current_s++;

	// main loop
	Timestamp i = 0;
	while ( (r[i].start != null_timepoint) || (s.start != null_timepoint) )
	{
		if ( (r[i].start < s.end) && (s.start < r[i].end) ) // overlap check
			result++;

		if ( (r[i].start != null_timepoint) && ( (s.start == null_timepoint) || (r[i].end <= s.end) ) )
		{
			// fetchRow(R)
			if (current_r[i] == end_r[i])
				r_nulls[i] = true;
			else
				r[i] = *(current_r[i])++;

			if (r_nulls[i])
				r[i].start = null_timepoint;
		}
		else
		{
			if (i < (m-1))
			{
				i++;
			}
			else
			{
				i = 0;

				// fetchRow(S)
				if (current_s == end_s)
					s_null = true;
				else
					s = *current_s++;

				if (s_null)
				{
					s.start = null_timepoint;
				}
			}
		}
	}

	// free and return result
	free(r_nulls);
	free(r);
	free(current_r);
	free(end_r);

	return result;
}

uint64_t dip_inner(Relation& R, Relation& S, Timestamp& domainStart, Timestamp& domainEnd)
{
	#ifdef TIMES
	Timer tim;
	tim.start();
	#endif

	std::vector<dip_heap_node> heap_r;
	create_dip( R, heap_r);
	std::vector<dip_heap_node> heap_s;
	create_dip( S, heap_s);

	#ifdef TIMES
	double timeCreateDip = tim.stop();
	std::cout << "CreateDIP time: " << timeCreateDip << std::endl;
	tim.start();
	#endif

	uint64_t result = 0;
	for (uint32_t j = 0; j < heap_s.size(); j++)
		result += dip_merge_inner( heap_r, heap_s[j].partition, domainStart, domainEnd);

	#ifdef TIMES
	double timeDipMerge = tim.stop();
	std::cout << "DipMerge time: " << timeDipMerge << std::endl;
	#endif

	return result;
}