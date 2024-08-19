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
#include "../containers/borders.hpp"

// used to get the id of an available thread
uint32_t getThreadId(bool& needsDetach, std::vector<uint32_t>& jobs);

struct structForParallelComplement
{
	uint32_t c;				// number of threads
	uint32_t chunk;				// thread id [0,c)
	Timestamp domainStart;			// domainStart value, which is needed for 1st complement tuple
	Timestamp domainEnd;			// domainEnd value, which is needed for last complement tuple
	ExtendedRelation* rel;			// relation to compute its complement
	Borders* borders;			// border information for relation
	std::vector<size_t>* each_group_sizes;	// array to save the size of each mini complement
	ExtendedRelation* complement;		// place to save complement
	Borders* borders_complement;		// border information to be set for complement relation
	uint32_t group_id;			// id of current group [0,groups_num-1]
	uint32_t group1;			// group1 value to compute complement
	uint32_t group2;			// group2 value to compute complement
	std::vector<uint32_t>* jobsList;	// list of threads - needs to be updated at the end of the computation
};

void* find_complement_sizes(void *args)
{
	structForParallelComplement* gained = (structForParallelComplement*) args;

	uint32_t count=0;
	Timestamp last = gained->domainStart;

	// find size of current group
	for (uint32_t i = (*gained->borders)[gained->group_id].position_start; i <= (*gained->borders)[gained->group_id].position_end; i++)
	{
		if (last < gained->rel->record_list[i].start)
		{
			last = gained->rel->record_list[i].end;
			count++;
		}
		else if (last < gained->rel->record_list[i].end)
		{
			last = gained->rel->record_list[i].end;
		}
	}
	if (last < gained->domainEnd)
	{
		count++;
	}

	// set size of current thread in the current group
	(*(gained->each_group_sizes))[gained->group_id] = count;

	// make current thread free to be used for next group
	(*(gained->jobsList))[gained->chunk] = 2;

	return NULL;
}

void* set_complement(void* args)
{
	structForParallelComplement* gained = (structForParallelComplement*) args;

	uint32_t point_to_write = (*(gained->each_group_sizes))[gained->group_id];
	Timestamp last = gained->domainStart;
	bool write_flag = false;

	// set complement
	for (uint32_t i = (*gained->borders)[gained->group_id].position_start; i <= (*gained->borders)[gained->group_id].position_end; i++)
	{
		if (last < gained->rel->record_list[i].start)
		{
			write_flag = true;
			gained->complement->record_list[point_to_write] = ExtendedRecord(last, gained->rel->record_list[i].start, gained->group1, gained->group2);
			last = gained->rel->record_list[i].end;
			point_to_write++;
		}
		else if (last < gained->rel->record_list[i].end)
		{
			last = gained->rel->record_list[i].end;
		}
	}
	if (last < gained->domainEnd)
	{
		write_flag = true;
		gained->complement->record_list[point_to_write] = ExtendedRecord(last, gained->domainEnd, gained->group1, gained->group2);
		point_to_write++;
	}

	// set borders for current group (set an error border in case of an empty complement)
	if (write_flag)
	{
		(*gained->borders_complement)[gained->group_id].group1 = (*gained->borders)[gained->group_id].group1;
		(*gained->borders_complement)[gained->group_id].group2 = (*gained->borders)[gained->group_id].group2;
		(*gained->borders_complement)[gained->group_id].position_start = (*(gained->each_group_sizes))[gained->group_id];
		(*gained->borders_complement)[gained->group_id].position_end = point_to_write - 1;
	}
	else
	{
		(*gained->borders_complement)[gained->group_id].group1 = (*gained->borders)[gained->group_id].group1;
		(*gained->borders_complement)[gained->group_id].group2 = (*gained->borders)[gained->group_id].group2; 
		(*gained->borders_complement)[gained->group_id].position_start = 1;
		(*gained->borders_complement)[gained->group_id].position_end = 0;
	}

	// make current thread free to be used for next group
	(*(gained->jobsList))[gained->chunk] = 2;

	return NULL;
}

void convert_to_complement( ExtendedRelation& R, Borders& borders, ExtendedRelation& complement, Borders& borders_complement,
							Timestamp foreignStart, Timestamp foreignEnd, uint32_t c)
{
	#ifdef TIMES
	Timer tim;
	tim.start();
	#endif

	structForParallelComplement toPass[c];
	pthread_t threads[c];
	bool needsDetach;
	uint32_t threadId;
	std::vector<uint32_t> jobsList( c, 1);

	// variables used for the commplement computation
	Timestamp domainStart = std::min( foreignStart, R.minStart);
	Timestamp domainEnd = std::max( foreignEnd, R.maxEnd);
	std::vector<size_t> each_group_sizes( borders.size(), 0);
	uint32_t current_group_id;
	borders_complement.resize( borders.size() );

	///////////////////////////////// find the size of complement /////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////

	fill( jobsList.begin(), jobsList.end(), 1);
	current_group_id = 0;
	for (auto& b : borders)
	{
		needsDetach = false;
		threadId = getThreadId(needsDetach, jobsList);
		if (needsDetach)
			if (pthread_detach(threads[threadId]))
				printf("Whoops\n");

		toPass[threadId].c = c;
		toPass[threadId].chunk = threadId;
		toPass[threadId].domainStart = domainStart;
		toPass[threadId].domainEnd = domainEnd;
		toPass[threadId].rel = &R;
		toPass[threadId].borders = &borders;
		toPass[threadId].each_group_sizes = &each_group_sizes;
		toPass[threadId].group_id = current_group_id;
		toPass[threadId].group1 = b.group1;
		toPass[threadId].group2 = b.group2;
		toPass[threadId].jobsList = &jobsList;
		pthread_create( &threads[threadId], NULL, find_complement_sizes, &toPass[threadId]);

		// next group
		current_group_id++;
	}
	for (uint32_t i=0; i < c; i++)
	{
		if (jobsList[i] != 1)
			pthread_join( threads[i], NULL);
	}
	////////////////////////////////////////////////////////////////////////////////////////////////

	// calculate full size of complement, change group sizes to points that each group should begin at new table
	uint32_t total = 0, previous_total;
	for (uint32_t i=0; i < borders.size(); i++)
	{
		previous_total = total;
		total += each_group_sizes[i];
		each_group_sizes[i] = previous_total;
	}
	complement.record_list = (ExtendedRecord*) malloc( total * sizeof(ExtendedRecord) );
	complement.numRecords = total;

	/////////////////////////////////////// set complement /////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////

	fill( jobsList.begin(), jobsList.end(), 1);
	current_group_id = 0;
	for (auto& b : borders)
	{
		needsDetach = false;
		threadId = getThreadId(needsDetach, jobsList);
		if (needsDetach)
			if (pthread_detach(threads[threadId]))
				printf("Whoops\n");

		toPass[threadId].c = c;
		toPass[threadId].chunk = threadId;
		toPass[threadId].domainStart = domainStart;
		toPass[threadId].domainEnd = domainEnd;
		toPass[threadId].rel = &R;
		toPass[threadId].borders = &borders;
		toPass[threadId].each_group_sizes = &each_group_sizes;
		toPass[threadId].complement = &complement;
		toPass[threadId].borders_complement = &borders_complement;
		toPass[threadId].group_id = current_group_id;
		toPass[threadId].group1 = b.group1;
		toPass[threadId].group2 = b.group2;
		toPass[threadId].jobsList = &jobsList;
		pthread_create( &threads[threadId], NULL, set_complement, &toPass[threadId]);

		// next group
		current_group_id++;
	}
	for (uint32_t i=0; i < c; i++)
	{
		if (jobsList[i] != 1)
			pthread_join( threads[i], NULL);
	}
	////////////////////////////////////////////////////////////////////////////////////////////////

	#ifdef TIMES
	double timeComplement = tim.stop();
	std::cout << "Complement time: " << timeComplement << " and size " << complement.numRecords << std::endl;
	#endif
}
