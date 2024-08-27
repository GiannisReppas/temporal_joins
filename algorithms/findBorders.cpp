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

/*
helper function -
for a range of [1,size], sets toTakeStart and toTakeEnd with equal subrange to be read, for thread with id chunk out of c total threads
*/
void chunk_to_read(uint32_t size, uint32_t c, uint32_t chunk, uint32_t &toTakeStart, uint32_t &toTakeEnd)
{
	uint32_t divResult = size / c;
	uint32_t modResult = size % c;
	toTakeStart = 1;
	for (uint32_t i=0; i < chunk; i++)
	{
		if (modResult <= i)
			toTakeStart += divResult;
		else
			toTakeStart += divResult+1;
	}
	if (modResult <= chunk)
		toTakeEnd = toTakeStart + divResult;
	else
		toTakeEnd = toTakeStart + divResult+1;
	toTakeEnd--;
}

struct structForParallelFindBorders
{
	uint32_t c;				// number of threads
	uint32_t chunk;				// thread id [0,c)
	ExtendedRelation *rel;			// relation to find its borders
	BordersElement* borders;	// linked list for border information in each relation chunk
	uint32_t *sizes;
};

void* find_borders_count_size(void* args)
{
	structForParallelFindBorders* gained = (structForParallelFindBorders*) args;

	// find chunk to read from S
	uint32_t toTakeStart, toTakeEnd;
	chunk_to_read( gained->rel->numRecords, gained->c, gained->chunk, toTakeStart, toTakeEnd);
	--toTakeStart;
	--toTakeEnd;

	// to handle edge case at which R.size() < c
	if (toTakeStart > toTakeEnd)
	{
		gained->sizes[ gained->chunk ] = 0;
		return NULL;
	}

	// find borders between [toTakeStart,toTakeEnd]
	uint32_t local_size = 0;
	if (
		(gained->chunk == 0) ||
		(gained->rel->record_list[toTakeStart].group1 != gained->rel->record_list[toTakeStart-1].group1) ||
		(gained->rel->record_list[toTakeStart].group2 != gained->rel->record_list[toTakeStart-1].group2)
	)
	{
		local_size++;
	}
	for (uint32_t i = toTakeStart+1; i <= toTakeEnd; i++)
	{
		if (
			(gained->rel->record_list[i].group1 != gained->rel->record_list[i-1].group1) ||
			(gained->rel->record_list[i].group2 != gained->rel->record_list[i-1].group2)
		)
		{
			local_size++;
		}
	}

	gained->sizes[ gained->chunk ] = local_size;

	return NULL;
}

void* find_borders_set(void* args)
{
	structForParallelFindBorders* gained = (structForParallelFindBorders*) args;

	// find chunk to read from S
	uint32_t toTakeStart, toTakeEnd;
	chunk_to_read( gained->rel->numRecords, gained->c, gained->chunk, toTakeStart, toTakeEnd);
	--toTakeStart;
	--toTakeEnd;

	// to handle edge case at which R.size() < c
	if (toTakeStart > toTakeEnd)
		return NULL;

	// find borders between [toTakeStart,toTakeEnd]
	uint32_t point_to_write = gained->sizes[gained->chunk];
	if (
		(gained->chunk == 0) ||
		(gained->rel->record_list[toTakeStart].group1 != gained->rel->record_list[toTakeStart-1].group1) ||
		(gained->rel->record_list[toTakeStart].group2 != gained->rel->record_list[toTakeStart-1].group2)
	)
	{
		if (gained->chunk != 0)
		{
			gained->borders[ point_to_write-1 ].position_end = toTakeStart-1;
		}

		gained->borders[ point_to_write ].group1 = gained->rel->record_list[toTakeStart].group1;
		gained->borders[ point_to_write ].group2 = gained->rel->record_list[toTakeStart].group2;
		gained->borders[ point_to_write ].position_start = toTakeStart;
		point_to_write++;
	}
	for (uint32_t i = toTakeStart+1; i <= toTakeEnd; i++)
	{
		if (
			(gained->rel->record_list[i].group1 != gained->rel->record_list[i-1].group1) ||
			(gained->rel->record_list[i].group2 != gained->rel->record_list[i-1].group2)
		)
		{
			gained->borders[ point_to_write-1 ].position_end = i-1;

			gained->borders[ point_to_write ].group1 = gained->rel->record_list[i].group1;
			gained->borders[ point_to_write ].group2 = gained->rel->record_list[i].group2;
			gained->borders[ point_to_write ].position_start = i;
			point_to_write++;
		}
	}

	return NULL;
}

void mainBorders( ExtendedRelation& R, Borders& bordersR, ExtendedRelation& S, Borders& bordersS, uint32_t c)
{
	#ifdef TIMES
	Timer tim;
	tim.start();
	#endif

	// variables to be used twice for each relation
	uint32_t total_size, previous_total;
	pthread_t threads[c];
	structForParallelFindBorders toPass[c];
	uint32_t *sizes;

	// find borders of each group in sorted R
	total_size = 0;
	sizes = (uint32_t*) malloc( c*sizeof(uint32_t) );
	for (uint32_t i = 0; i < c; i++)
	{
		toPass[i].c = c;
		toPass[i].chunk = i;
		toPass[i].rel = &R;
		toPass[i].sizes = sizes;
		pthread_create( &threads[i], NULL, find_borders_count_size, &toPass[i]);
	}
	previous_total = 0;
	for (uint32_t i = 0; i < c; i++)
	{
		pthread_join( threads[i], NULL);
		total_size += sizes[i];
		sizes[i] = previous_total;
		previous_total = total_size;
	}
	bordersR.borders_list = (BordersElement*) malloc( total_size*sizeof(BordersElement) );
	bordersR.numBorders = total_size;
	for (uint32_t i = 0; i < c; i++)
	{
		toPass[i].c = c;
		toPass[i].chunk = i;
		toPass[i].rel = &R;
		toPass[i].borders = bordersR.borders_list;
		toPass[i].sizes = sizes;
		pthread_create( &threads[i], NULL, find_borders_set, &toPass[i]);
	}
	for (uint32_t i = 0; i < c; i++)
	{
		pthread_join( threads[i], NULL);
	}
	bordersR.borders_list[ bordersR.numBorders-1 ].position_end = R.numRecords-1;
	free(sizes);

	// find borders of each group in sorted S
	total_size = 0;
	sizes = (uint32_t*) malloc( c*sizeof(uint32_t) );
	for (uint32_t i = 0; i < c; i++)
	{
		toPass[i].c = c;
		toPass[i].chunk = i;
		toPass[i].rel = &S;
		toPass[i].sizes = sizes;
		pthread_create( &threads[i], NULL, find_borders_count_size, &toPass[i]);
	}
	previous_total = 0;
	for (uint32_t i = 0; i < c; i++)
	{
		pthread_join( threads[i], NULL);
		total_size += sizes[i];
		sizes[i] = previous_total;
		previous_total = total_size;
	}
	bordersS.borders_list = (BordersElement*) malloc( total_size*sizeof(BordersElement) );
	bordersS.numBorders = total_size;
	for (uint32_t i = 0; i < c; i++)
	{
		toPass[i].c = c;
		toPass[i].chunk = i;
		toPass[i].rel = &S;
		toPass[i].borders = bordersS.borders_list;
		toPass[i].sizes = sizes;
		pthread_create( &threads[i], NULL, find_borders_set, &toPass[i]);
	}
	for (uint32_t i = 0; i < c; i++)
	{
		pthread_join( threads[i], NULL);
	}
	bordersS.borders_list[ bordersS.numBorders-1 ].position_end = S.numRecords-1;
	free(sizes);

	#ifdef TIMES
	double timeFindBorders = tim.stop();
	std::cout << "FindBorders time: " << timeFindBorders << std::endl;
	#endif
}
