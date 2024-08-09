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
	std::vector< Borders > *localBorders;	// linked list for border information in each relation chunk
};

void* find_borders(void* args)
{
	structForParallelFindBorders* gained = (structForParallelFindBorders*) args;

	// find chunk to read from S
	uint32_t toTakeStart, toTakeEnd;
	chunk_to_read( gained->rel->size(), gained->c, gained->chunk, toTakeStart, toTakeEnd);
	--toTakeStart;
	--toTakeEnd;

	// to handle edge case at which R.size() < c
	if (toTakeStart > toTakeEnd)
		return NULL;

	// find borders between [toTakeStart,toTakeEnd]
	Timestamp last = toTakeStart;
	for (uint32_t i = toTakeStart; i < toTakeEnd; i++)
	{
		if ( ((*(gained->rel))[i].group1 != (*(gained->rel))[i+1].group1) || ((*(gained->rel))[i].group2 != (*(gained->rel))[i+1].group2) )
		{
			(*(gained->localBorders))[gained->chunk].push_back( BordersElement( (*(gained->rel))[last].group1, (*(gained->rel))[last].group2, last, i) );
			last = i + 1;
		}
	}
	(*(gained->localBorders))[gained->chunk].push_back( BordersElement( (*(gained->rel))[last].group1, (*(gained->rel))[last].group2, last, toTakeEnd) );

	return NULL;
}

void mainBorders( ExtendedRelation& R, Borders& bordersR, ExtendedRelation& S, Borders& bordersS, uint32_t c)
{
	#ifdef TIMES
	Timer tim;
	tim.start();
	#endif

	pthread_t threads[c];
	structForParallelFindBorders toPass[c];

	// find borders of each group in sorted R
	std::vector< Borders > localBordersR(c);
	for (uint32_t i=0; i < c; i++)
	{
		toPass[i].c = c;
		toPass[i].chunk = i;
		toPass[i].rel = &R;
		toPass[i].localBorders = &localBordersR;
		pthread_create( &threads[i], NULL, find_borders, &toPass[i]);
	}
	for (uint32_t i=0; i < c; i++)
	{
		pthread_join( threads[i], NULL);
	}
	// merge linked lists
	bordersR.insert( bordersR.end(), localBordersR[0].begin(), localBordersR[0].end() );
	for (uint32_t i=1,j; i < c; i++)
	{
		// to handle edge case at which R.size() < c
		if (localBordersR[i].size() == 0)
			break;

		// append current linked list to global linked list (merge first with last borders if needed)
		j = 0;
		if ( (localBordersR[i][j].group1 == (bordersR.end()-1)->group1) && (localBordersR[i][j].group2 == (bordersR.end()-1)->group2) )
		{
			(bordersR.end()-1)->position_end = localBordersR[i][j].position_end;
			j = 1;
		}
		bordersR.insert( bordersR.end(), localBordersR[i].begin() + j, localBordersR[i].end() );
	}

	// find borders of each group in sorted S
	std::vector< Borders > localBordersS(c);
	for (uint32_t i=0; i < c; i++)
	{
		toPass[i].c = c;
		toPass[i].chunk = i;
		toPass[i].rel = &S;
		toPass[i].localBorders = &localBordersS;
		pthread_create( &threads[i], NULL, find_borders, &toPass[i]);
	}
	for (uint32_t i=0; i < c; i++)
	{
		pthread_join( threads[i], NULL);
	}
	// merge linked lists
	bordersS.insert( bordersS.end(), localBordersS[0].begin(), localBordersS[0].end() );
	for (uint32_t i=1,j; i < c; i++)
	{
		// to handle edge case at which R.size() < c
		if (localBordersS[i].size() == 0)
			break;

		// append current linked list to global linked list (merge first with last borders if needed)
		j = 0;
		if ( (localBordersS[i][j].group1 == (bordersS.end()-1)->group1) && (localBordersS[i][j].group2 == (bordersS.end()-1)->group2) )
		{
			(bordersS.end()-1)->position_end = localBordersS[i][j].position_end;
			j = 1;
		}
		bordersS.insert( bordersS.end(), localBordersS[i].begin() + j, localBordersS[i].end() );
	}

	#ifdef TIMES
	double timeFindBorders = tim.stop();
	std::cout << "FindBorders time: " << timeFindBorders << std::endl;
	#endif
}
