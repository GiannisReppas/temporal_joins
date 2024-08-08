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

bool CompareByEnd(const Record& lhs, const Record& rhs)
{
	return (lhs.end < rhs.end);
}

////////////////////
// Internal loops //
////////////////////

inline unsigned long long bguFS_InternalLoop(Group &G, RelationIterator firstFS, RelationIterator lastFS, const BucketIndex &BI, Timestamp minStart)
{
	unsigned long long result = 0;
	long int cbucket_id, pbucket_id;
	
	
	auto pivot = firstFS;
	auto lastG = G.end();
	for (auto curr = G.begin(); curr != lastG; curr++)
	{
		auto bufferSize = (lastG-curr);
		
		if (pivot == lastFS)
			break;
		if (curr->end < minStart)
			continue;
		
		cbucket_id = ceil((double)(curr->end-minStart)/BI.bucket_range);
		if (cbucket_id >= BI.numBuckets)
			cbucket_id = BI.numBuckets-1;
		pbucket_id = ceil((double)(pivot->end-minStart)/BI.bucket_range);
		if (pbucket_id >= BI.numBuckets)
			pbucket_id = BI.numBuckets-1;
		
		if (cbucket_id > pbucket_id)
		{
			auto last = BI[cbucket_id-1].last;
			switch (bufferSize)
			{
				case 1:
					while (last-pivot >= 32)
					{
#ifdef WORKLOAD_COUNT
						result += 32;
#else
						result += curr->start ^ (pivot+0)->start;
						result += curr->start ^ (pivot+1)->start;
						result += curr->start ^ (pivot+2)->start;
						result += curr->start ^ (pivot+3)->start;
						result += curr->start ^ (pivot+4)->start;
						result += curr->start ^ (pivot+5)->start;
						result += curr->start ^ (pivot+6)->start;
						result += curr->start ^ (pivot+7)->start;
						result += curr->start ^ (pivot+8)->start;
						result += curr->start ^ (pivot+9)->start;
						result += curr->start ^ (pivot+10)->start;
						result += curr->start ^ (pivot+11)->start;
						result += curr->start ^ (pivot+12)->start;
						result += curr->start ^ (pivot+13)->start;
						result += curr->start ^ (pivot+14)->start;
						result += curr->start ^ (pivot+15)->start;
						result += curr->start ^ (pivot+16)->start;
						result += curr->start ^ (pivot+17)->start;
						result += curr->start ^ (pivot+18)->start;
						result += curr->start ^ (pivot+19)->start;
						result += curr->start ^ (pivot+20)->start;
						result += curr->start ^ (pivot+21)->start;
						result += curr->start ^ (pivot+22)->start;
						result += curr->start ^ (pivot+23)->start;
						result += curr->start ^ (pivot+24)->start;
						result += curr->start ^ (pivot+25)->start;
						result += curr->start ^ (pivot+26)->start;
						result += curr->start ^ (pivot+27)->start;
						result += curr->start ^ (pivot+28)->start;
						result += curr->start ^ (pivot+29)->start;
						result += curr->start ^ (pivot+30)->start;
						result += curr->start ^ (pivot+31)->start;
#endif
						pivot += 32;
					}
					
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result++;
#else
						result += curr->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 2:
					while (last-pivot >= 16)
					{
#ifdef WORKLOAD_COUNT
						result += 32;
#else
						result += (curr+0)->start ^ (pivot+0)->start;
						result += (curr+1)->start ^ (pivot+0)->start;
						result += (curr+0)->start ^ (pivot+1)->start;
						result += (curr+1)->start ^ (pivot+1)->start;
						result += (curr+0)->start ^ (pivot+2)->start;
						result += (curr+1)->start ^ (pivot+2)->start;
						result += (curr+0)->start ^ (pivot+3)->start;
						result += (curr+1)->start ^ (pivot+3)->start;
						result += (curr+0)->start ^ (pivot+4)->start;
						result += (curr+1)->start ^ (pivot+4)->start;
						result += (curr+0)->start ^ (pivot+5)->start;
						result += (curr+1)->start ^ (pivot+5)->start;
						result += (curr+0)->start ^ (pivot+6)->start;
						result += (curr+1)->start ^ (pivot+6)->start;
						result += (curr+0)->start ^ (pivot+7)->start;
						result += (curr+1)->start ^ (pivot+7)->start;
						result += (curr+0)->start ^ (pivot+8)->start;
						result += (curr+1)->start ^ (pivot+8)->start;
						result += (curr+0)->start ^ (pivot+9)->start;
						result += (curr+1)->start ^ (pivot+9)->start;
						result += (curr+0)->start ^ (pivot+10)->start;
						result += (curr+1)->start ^ (pivot+10)->start;
						result += (curr+0)->start ^ (pivot+11)->start;
						result += (curr+1)->start ^ (pivot+11)->start;
						result += (curr+0)->start ^ (pivot+12)->start;
						result += (curr+1)->start ^ (pivot+12)->start;
						result += (curr+0)->start ^ (pivot+13)->start;
						result += (curr+1)->start ^ (pivot+13)->start;
						result += (curr+0)->start ^ (pivot+14)->start;
						result += (curr+1)->start ^ (pivot+14)->start;
						result += (curr+0)->start ^ (pivot+15)->start;
						result += (curr+1)->start ^ (pivot+15)->start;
#endif
						pivot += 16;
					}
					
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 2;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 3:
					while (last-pivot >= 8)
					{
#ifdef WORKLOAD_COUNT
						result += 24;
#else
						result += (curr+0)->start ^ (pivot+0)->start;
						result += (curr+1)->start ^ (pivot+0)->start;
						result += (curr+2)->start ^ (pivot+0)->start;
						result += (curr+0)->start ^ (pivot+1)->start;
						result += (curr+1)->start ^ (pivot+1)->start;
						result += (curr+2)->start ^ (pivot+1)->start;
						result += (curr+0)->start ^ (pivot+2)->start;
						result += (curr+1)->start ^ (pivot+2)->start;
						result += (curr+2)->start ^ (pivot+2)->start;
						result += (curr+0)->start ^ (pivot+3)->start;
						result += (curr+1)->start ^ (pivot+3)->start;
						result += (curr+2)->start ^ (pivot+3)->start;
						result += (curr+0)->start ^ (pivot+4)->start;
						result += (curr+1)->start ^ (pivot+4)->start;
						result += (curr+2)->start ^ (pivot+4)->start;
						result += (curr+0)->start ^ (pivot+5)->start;
						result += (curr+1)->start ^ (pivot+5)->start;
						result += (curr+2)->start ^ (pivot+5)->start;
						result += (curr+0)->start ^ (pivot+6)->start;
						result += (curr+1)->start ^ (pivot+6)->start;
						result += (curr+2)->start ^ (pivot+6)->start;
						result += (curr+0)->start ^ (pivot+7)->start;
						result += (curr+1)->start ^ (pivot+7)->start;
						result += (curr+2)->start ^ (pivot+7)->start;
#endif
						pivot += 8;
					}
					
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 3;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
#endif						
						pivot++;
					}
					break;
					
				case 4:
					while (last-pivot >= 8)
					{
#ifdef WORKLOAD_COUNT
						result += 32;
#else
						result += (curr+0)->start ^ (pivot+0)->start;
						result += (curr+1)->start ^ (pivot+0)->start;
						result += (curr+2)->start ^ (pivot+0)->start;
						result += (curr+3)->start ^ (pivot+0)->start;
						result += (curr+0)->start ^ (pivot+1)->start;
						result += (curr+1)->start ^ (pivot+1)->start;
						result += (curr+2)->start ^ (pivot+1)->start;
						result += (curr+3)->start ^ (pivot+1)->start;
						result += (curr+0)->start ^ (pivot+2)->start;
						result += (curr+1)->start ^ (pivot+2)->start;
						result += (curr+2)->start ^ (pivot+2)->start;
						result += (curr+3)->start ^ (pivot+2)->start;
						result += (curr+0)->start ^ (pivot+3)->start;
						result += (curr+1)->start ^ (pivot+3)->start;
						result += (curr+2)->start ^ (pivot+3)->start;
						result += (curr+3)->start ^ (pivot+3)->start;
						result += (curr+0)->start ^ (pivot+4)->start;
						result += (curr+1)->start ^ (pivot+4)->start;
						result += (curr+2)->start ^ (pivot+4)->start;
						result += (curr+3)->start ^ (pivot+4)->start;
						result += (curr+0)->start ^ (pivot+5)->start;
						result += (curr+1)->start ^ (pivot+5)->start;
						result += (curr+2)->start ^ (pivot+5)->start;
						result += (curr+3)->start ^ (pivot+5)->start;
						result += (curr+0)->start ^ (pivot+6)->start;
						result += (curr+1)->start ^ (pivot+6)->start;
						result += (curr+2)->start ^ (pivot+6)->start;
						result += (curr+3)->start ^ (pivot+6)->start;
						result += (curr+0)->start ^ (pivot+7)->start;
						result += (curr+1)->start ^ (pivot+7)->start;
						result += (curr+2)->start ^ (pivot+7)->start;
						result += (curr+3)->start ^ (pivot+7)->start;
#endif
						pivot += 8;
					}
					
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 4;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 5:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 5;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 6:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 6;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 7:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 7;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 8:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 8;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 9:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 9;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 10:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 10;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 11:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 11;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 12:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 12;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 13:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 13;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 14:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 14;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 15:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 15;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 16:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 16;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 17:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 17;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 18:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 18;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 19:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 19;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 20:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 20;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 21:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 21;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 22:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 22;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 23:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 23;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 24:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 24;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 25:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 25;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 26:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 26;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 27:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 27;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 28:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 28;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
#endif						
						pivot++;
					}
					break;
					
				case 29:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 29;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
						result += (curr+28)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 30:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result +=30;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
						result += (curr+28)->start ^ pivot->start;
						result += (curr+29)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 31:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 31;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
						result += (curr+28)->start ^ pivot->start;
						result += (curr+29)->start ^ pivot->start;
						result += (curr+30)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				case 32:
					while (pivot < last)
					{
#ifdef WORKLOAD_COUNT
						result += 32;
#else
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
						result += (curr+28)->start ^ pivot->start;
						result += (curr+29)->start ^ pivot->start;
						result += (curr+30)->start ^ pivot->start;
						result += (curr+31)->start ^ pivot->start;
#endif
						pivot++;
					}
					break;
					
				default:
					while (last-pivot >= 32)
					{
						for (auto k = curr; k != lastG; k++)
						{
#ifdef WORKLOAD_COUNT
							result += 32;
#else
							result += k->start ^ (pivot+0)->start;
							result += k->start ^ (pivot+1)->start;
							result += k->start ^ (pivot+2)->start;
							result += k->start ^ (pivot+3)->start;
							result += k->start ^ (pivot+4)->start;
							result += k->start ^ (pivot+5)->start;
							result += k->start ^ (pivot+6)->start;
							result += k->start ^ (pivot+7)->start;
							result += k->start ^ (pivot+8)->start;
							result += k->start ^ (pivot+9)->start;
							result += k->start ^ (pivot+10)->start;
							result += k->start ^ (pivot+11)->start;
							result += k->start ^ (pivot+12)->start;
							result += k->start ^ (pivot+13)->start;
							result += k->start ^ (pivot+14)->start;
							result += k->start ^ (pivot+15)->start;
							result += k->start ^ (pivot+16)->start;
							result += k->start ^ (pivot+17)->start;
							result += k->start ^ (pivot+18)->start;
							result += k->start ^ (pivot+19)->start;
							result += k->start ^ (pivot+20)->start;
							result += k->start ^ (pivot+21)->start;
							result += k->start ^ (pivot+22)->start;
							result += k->start ^ (pivot+23)->start;
							result += k->start ^ (pivot+24)->start;
							result += k->start ^ (pivot+25)->start;
							result += k->start ^ (pivot+26)->start;
							result += k->start ^ (pivot+27)->start;
							result += k->start ^ (pivot+28)->start;
							result += k->start ^ (pivot+29)->start;
							result += k->start ^ (pivot+30)->start;
							result += k->start ^ (pivot+31)->start;
#endif
						}
						pivot += 32;
					}
					
					if (last-pivot >= 16)
					{
						for (auto k = curr; k != lastG; k++)
						{
#ifdef WORKLOAD_COUNT
							result += 16;
#else
							result += k->start ^ (pivot+0)->start;
							result += k->start ^ (pivot+1)->start;
							result += k->start ^ (pivot+2)->start;
							result += k->start ^ (pivot+3)->start;
							result += k->start ^ (pivot+4)->start;
							result += k->start ^ (pivot+5)->start;
							result += k->start ^ (pivot+6)->start;
							result += k->start ^ (pivot+7)->start;
							result += k->start ^ (pivot+8)->start;
							result += k->start ^ (pivot+9)->start;
							result += k->start ^ (pivot+10)->start;
							result += k->start ^ (pivot+11)->start;
							result += k->start ^ (pivot+12)->start;
							result += k->start ^ (pivot+13)->start;
							result += k->start ^ (pivot+14)->start;
							result += k->start ^ (pivot+15)->start;
#endif
						}
						pivot += 16;
					}
					
					if (last-pivot >= 8)
					{
						for (auto k = curr; k != lastG; k++)
						{
#ifdef WORKLOAD_COUNT
							result += 8;
#else
							result += k->start ^ (pivot+0)->start;
							result += k->start ^ (pivot+1)->start;
							result += k->start ^ (pivot+2)->start;
							result += k->start ^ (pivot+3)->start;
							result += k->start ^ (pivot+4)->start;
							result += k->start ^ (pivot+5)->start;
							result += k->start ^ (pivot+6)->start;
							result += k->start ^ (pivot+7)->start;
#endif
						}
						pivot += 8;
					}
					
					if (last-pivot >= 4)
					{
						for (auto k = curr; k != lastG; k++)
						{
#ifdef WORKLOAD_COUNT
							result += 4;
#else
							result += k->start ^ (pivot+0)->start;
							result += k->start ^ (pivot+1)->start;
							result += k->start ^ (pivot+2)->start;
							result += k->start ^ (pivot+3)->start;
#endif
						}
						pivot += 4;
					}
					
					while (pivot < last)
					{
						auto k = curr;
						while (lastG-k >= 32)
						{
#ifdef WORKLOAD_COUNT
							result += 32;
#else
							result += (k+0)->start ^ pivot->start;
							result += (k+1)->start ^ pivot->start;
							result += (k+2)->start ^ pivot->start;
							result += (k+3)->start ^ pivot->start;
							result += (k+4)->start ^ pivot->start;
							result += (k+5)->start ^ pivot->start;
							result += (k+6)->start ^ pivot->start;
							result += (k+7)->start ^ pivot->start;
							result += (k+8)->start ^ pivot->start;
							result += (k+9)->start ^ pivot->start;
							result += (k+10)->start ^ pivot->start;
							result += (k+11)->start ^ pivot->start;
							result += (k+12)->start ^ pivot->start;
							result += (k+13)->start ^ pivot->start;
							result += (k+14)->start ^ pivot->start;
							result += (k+15)->start ^ pivot->start;
							result += (k+16)->start ^ pivot->start;
							result += (k+17)->start ^ pivot->start;
							result += (k+18)->start ^ pivot->start;
							result += (k+19)->start ^ pivot->start;
							result += (k+20)->start ^ pivot->start;
							result += (k+21)->start ^ pivot->start;
							result += (k+22)->start ^ pivot->start;
							result += (k+23)->start ^ pivot->start;
							result += (k+24)->start ^ pivot->start;
							result += (k+25)->start ^ pivot->start;
							result += (k+26)->start ^ pivot->start;
							result += (k+27)->start ^ pivot->start;
							result += (k+28)->start ^ pivot->start;
							result += (k+29)->start ^ pivot->start;
							result += (k+30)->start ^ pivot->start;
							result += (k+31)->start ^ pivot->start;
#endif
							k += 32;
						}
						
						if (lastG-k >= 16)
						{
#ifdef WORKLOAD_COUNT
							result += 16;
#else
							result += (k+0)->start ^ pivot->start;
							result += (k+1)->start ^ pivot->start;
							result += (k+2)->start ^ pivot->start;
							result += (k+3)->start ^ pivot->start;
							result += (k+4)->start ^ pivot->start;
							result += (k+5)->start ^ pivot->start;
							result += (k+6)->start ^ pivot->start;
							result += (k+7)->start ^ pivot->start;
							result += (k+8)->start ^ pivot->start;
							result += (k+9)->start ^ pivot->start;
							result += (k+10)->start ^ pivot->start;
							result += (k+11)->start ^ pivot->start;
							result += (k+12)->start ^ pivot->start;
							result += (k+13)->start ^ pivot->start;
							result += (k+14)->start ^ pivot->start;
							result += (k+15)->start ^ pivot->start;
#endif
							k += 16;
						}
						
						if (lastG-k >= 8)
						{
#ifdef WORKLOAD_COUNT
							result += 8;
#else
							result += (k+0)->start ^ pivot->start;
							result += (k+1)->start ^ pivot->start;
							result += (k+2)->start ^ pivot->start;
							result += (k+3)->start ^ pivot->start;
							result += (k+4)->start ^ pivot->start;
							result += (k+5)->start ^ pivot->start;
							result += (k+6)->start ^ pivot->start;
							result += (k+7)->start ^ pivot->start;
#endif
							k += 8;
						}
						
						if (lastG-k >= 4)
						{
#ifdef WORKLOAD_COUNT
							result += 4;
#else
							result += (k+0)->start ^ pivot->start;
							result += (k+1)->start ^ pivot->start;
							result += (k+2)->start ^ pivot->start;
							result += (k+3)->start ^ pivot->start;
#endif
							k += 4;
						}
						
						while (k != lastG)
						{
#ifdef WORKLOAD_COUNT
							result++;
#else
							result += k->start ^ pivot->start;
#endif
							k++;
						}
						
						pivot++;
					}
					break;
			}
		}
		
		// Sweep the last bucket.
		auto last = BI[cbucket_id].last;
		switch (bufferSize)
		{
			case 1:
				while ((last-pivot >= 32) && (curr->end > (pivot+31)->start))
				{
#ifdef WORKLOAD_COUNT
					result += 32;
#else
					result += curr->start ^ (pivot+0)->start;
					result += curr->start ^ (pivot+1)->start;
					result += curr->start ^ (pivot+2)->start;
					result += curr->start ^ (pivot+3)->start;
					result += curr->start ^ (pivot+4)->start;
					result += curr->start ^ (pivot+5)->start;
					result += curr->start ^ (pivot+6)->start;
					result += curr->start ^ (pivot+7)->start;
					result += curr->start ^ (pivot+8)->start;
					result += curr->start ^ (pivot+9)->start;
					result += curr->start ^ (pivot+10)->start;
					result += curr->start ^ (pivot+11)->start;
					result += curr->start ^ (pivot+12)->start;
					result += curr->start ^ (pivot+13)->start;
					result += curr->start ^ (pivot+14)->start;
					result += curr->start ^ (pivot+15)->start;
					result += curr->start ^ (pivot+16)->start;
					result += curr->start ^ (pivot+17)->start;
					result += curr->start ^ (pivot+18)->start;
					result += curr->start ^ (pivot+19)->start;
					result += curr->start ^ (pivot+20)->start;
					result += curr->start ^ (pivot+21)->start;
					result += curr->start ^ (pivot+22)->start;
					result += curr->start ^ (pivot+23)->start;
					result += curr->start ^ (pivot+24)->start;
					result += curr->start ^ (pivot+25)->start;
					result += curr->start ^ (pivot+26)->start;
					result += curr->start ^ (pivot+27)->start;
					result += curr->start ^ (pivot+28)->start;
					result += curr->start ^ (pivot+29)->start;
					result += curr->start ^ (pivot+30)->start;
					result += curr->start ^ (pivot+31)->start;
#endif
					pivot += 32;
				}
				
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result++;
#else
					result += curr->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 2:
				while ((last-pivot >= 16) && (curr->end > (pivot+15)->start))
				{
#ifdef WORKLOAD_COUNT
					result += 32;
#else
					result += (curr+0)->start ^ (pivot+0)->start;
					result += (curr+1)->start ^ (pivot+0)->start;
					result += (curr+0)->start ^ (pivot+1)->start;
					result += (curr+1)->start ^ (pivot+1)->start;
					result += (curr+0)->start ^ (pivot+2)->start;
					result += (curr+1)->start ^ (pivot+2)->start;
					result += (curr+0)->start ^ (pivot+3)->start;
					result += (curr+1)->start ^ (pivot+3)->start;
					result += (curr+0)->start ^ (pivot+4)->start;
					result += (curr+1)->start ^ (pivot+4)->start;
					result += (curr+0)->start ^ (pivot+5)->start;
					result += (curr+1)->start ^ (pivot+5)->start;
					result += (curr+0)->start ^ (pivot+6)->start;
					result += (curr+1)->start ^ (pivot+6)->start;
					result += (curr+0)->start ^ (pivot+7)->start;
					result += (curr+1)->start ^ (pivot+7)->start;
					result += (curr+0)->start ^ (pivot+8)->start;
					result += (curr+1)->start ^ (pivot+8)->start;
					result += (curr+0)->start ^ (pivot+9)->start;
					result += (curr+1)->start ^ (pivot+9)->start;
					result += (curr+0)->start ^ (pivot+10)->start;
					result += (curr+1)->start ^ (pivot+10)->start;
					result += (curr+0)->start ^ (pivot+11)->start;
					result += (curr+1)->start ^ (pivot+11)->start;
					result += (curr+0)->start ^ (pivot+12)->start;
					result += (curr+1)->start ^ (pivot+12)->start;
					result += (curr+0)->start ^ (pivot+13)->start;
					result += (curr+1)->start ^ (pivot+13)->start;
					result += (curr+0)->start ^ (pivot+14)->start;
					result += (curr+1)->start ^ (pivot+14)->start;
					result += (curr+0)->start ^ (pivot+15)->start;
					result += (curr+1)->start ^ (pivot+15)->start;
#endif
					pivot += 16;
				}
				
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 2;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 3:
				while ((last-pivot >= 8) && (curr->end > (pivot+7)->start))
				{
#ifdef WORKLOAD_COUNT
					result += 24;
#else
					result += (curr+0)->start ^ (pivot+0)->start;
					result += (curr+1)->start ^ (pivot+0)->start;
					result += (curr+2)->start ^ (pivot+0)->start;
					result += (curr+0)->start ^ (pivot+1)->start;
					result += (curr+1)->start ^ (pivot+1)->start;
					result += (curr+2)->start ^ (pivot+1)->start;
					result += (curr+0)->start ^ (pivot+2)->start;
					result += (curr+1)->start ^ (pivot+2)->start;
					result += (curr+2)->start ^ (pivot+2)->start;
					result += (curr+0)->start ^ (pivot+3)->start;
					result += (curr+1)->start ^ (pivot+3)->start;
					result += (curr+2)->start ^ (pivot+3)->start;
					result += (curr+0)->start ^ (pivot+4)->start;
					result += (curr+1)->start ^ (pivot+4)->start;
					result += (curr+2)->start ^ (pivot+4)->start;
					result += (curr+0)->start ^ (pivot+5)->start;
					result += (curr+1)->start ^ (pivot+5)->start;
					result += (curr+2)->start ^ (pivot+5)->start;
					result += (curr+0)->start ^ (pivot+6)->start;
					result += (curr+1)->start ^ (pivot+6)->start;
					result += (curr+2)->start ^ (pivot+6)->start;
					result += (curr+0)->start ^ (pivot+7)->start;
					result += (curr+1)->start ^ (pivot+7)->start;
					result += (curr+2)->start ^ (pivot+7)->start;
#endif
					pivot += 8;
				}
				
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 3;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 4:
				while ((last-pivot >= 8) && (curr->end > (pivot+7)->start))
				{
#ifdef WORKLOAD_COUNT
					result += 32;
#else
					result += (curr+0)->start ^ (pivot+0)->start;
					result += (curr+1)->start ^ (pivot+0)->start;
					result += (curr+2)->start ^ (pivot+0)->start;
					result += (curr+3)->start ^ (pivot+0)->start;
					result += (curr+0)->start ^ (pivot+1)->start;
					result += (curr+1)->start ^ (pivot+1)->start;
					result += (curr+2)->start ^ (pivot+1)->start;
					result += (curr+3)->start ^ (pivot+1)->start;
					result += (curr+0)->start ^ (pivot+2)->start;
					result += (curr+1)->start ^ (pivot+2)->start;
					result += (curr+2)->start ^ (pivot+2)->start;
					result += (curr+3)->start ^ (pivot+2)->start;
					result += (curr+0)->start ^ (pivot+3)->start;
					result += (curr+1)->start ^ (pivot+3)->start;
					result += (curr+2)->start ^ (pivot+3)->start;
					result += (curr+3)->start ^ (pivot+3)->start;
					result += (curr+0)->start ^ (pivot+4)->start;
					result += (curr+1)->start ^ (pivot+4)->start;
					result += (curr+2)->start ^ (pivot+4)->start;
					result += (curr+3)->start ^ (pivot+4)->start;
					result += (curr+0)->start ^ (pivot+5)->start;
					result += (curr+1)->start ^ (pivot+5)->start;
					result += (curr+2)->start ^ (pivot+5)->start;
					result += (curr+3)->start ^ (pivot+5)->start;
					result += (curr+0)->start ^ (pivot+6)->start;
					result += (curr+1)->start ^ (pivot+6)->start;
					result += (curr+2)->start ^ (pivot+6)->start;
					result += (curr+3)->start ^ (pivot+6)->start;
					result += (curr+0)->start ^ (pivot+7)->start;
					result += (curr+1)->start ^ (pivot+7)->start;
					result += (curr+2)->start ^ (pivot+7)->start;
					result += (curr+3)->start ^ (pivot+7)->start;
#endif
					pivot += 8;
				}
				
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 4;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 5:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 5;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 6:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 6;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 7:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 7;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 8:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 8;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 9:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 9;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 10:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 10;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 11:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 11;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 12:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 12;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 13:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 13;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 14:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 14;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 15:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 15;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 16:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 16;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 17:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 17;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 18:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 18;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 19:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 19;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 20:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 20;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 21:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 21;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 22:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 22;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 23:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 23;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 24:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 24;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 25:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 25;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 26:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 26;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 27:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 27;
#else					
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 28:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 28;
#else					
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 29:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 29;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
					result += (curr+28)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 30:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 30;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
					result += (curr+28)->start ^ pivot->start;
					result += (curr+29)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 31:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 31;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
					result += (curr+28)->start ^ pivot->start;
					result += (curr+29)->start ^ pivot->start;
					result += (curr+30)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			case 32:
				while ((pivot < last) && (pivot->start < curr->end))
				{
#ifdef WORKLOAD_COUNT
					result += 32;
#else
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
					result += (curr+28)->start ^ pivot->start;
					result += (curr+29)->start ^ pivot->start;
					result += (curr+30)->start ^ pivot->start;
					result += (curr+31)->start ^ pivot->start;
#endif
					pivot++;
				}
				break;
				
			default:
				while ((last-pivot >= 32) && (curr->end > (pivot+31)->start))
				{
					for (auto k = curr; k != lastG; k++)
					{
#ifdef WORKLOAD_COUNT
						result += 32;
#else
						result += k->start ^ (pivot+0)->start;
						result += k->start ^ (pivot+1)->start;
						result += k->start ^ (pivot+2)->start;
						result += k->start ^ (pivot+3)->start;
						result += k->start ^ (pivot+4)->start;
						result += k->start ^ (pivot+5)->start;
						result += k->start ^ (pivot+6)->start;
						result += k->start ^ (pivot+7)->start;
						result += k->start ^ (pivot+8)->start;
						result += k->start ^ (pivot+9)->start;
						result += k->start ^ (pivot+10)->start;
						result += k->start ^ (pivot+11)->start;
						result += k->start ^ (pivot+12)->start;
						result += k->start ^ (pivot+13)->start;
						result += k->start ^ (pivot+14)->start;
						result += k->start ^ (pivot+15)->start;
						result += k->start ^ (pivot+16)->start;
						result += k->start ^ (pivot+17)->start;
						result += k->start ^ (pivot+18)->start;
						result += k->start ^ (pivot+19)->start;
						result += k->start ^ (pivot+20)->start;
						result += k->start ^ (pivot+21)->start;
						result += k->start ^ (pivot+22)->start;
						result += k->start ^ (pivot+23)->start;
						result += k->start ^ (pivot+24)->start;
						result += k->start ^ (pivot+25)->start;
						result += k->start ^ (pivot+26)->start;
						result += k->start ^ (pivot+27)->start;
						result += k->start ^ (pivot+28)->start;
						result += k->start ^ (pivot+29)->start;
						result += k->start ^ (pivot+30)->start;
						result += k->start ^ (pivot+31)->start;
#endif
					}
					pivot += 32;
				}
				
				if ((last-pivot >= 16) && (curr->end > (pivot+15)->start))
				{
					for (auto k = curr; k != lastG; k++)
					{
#ifdef WORKLOAD_COUNT
						result += 16;
#else
						result += k->start ^ (pivot+0)->start;
						result += k->start ^ (pivot+1)->start;
						result += k->start ^ (pivot+2)->start;
						result += k->start ^ (pivot+3)->start;
						result += k->start ^ (pivot+4)->start;
						result += k->start ^ (pivot+5)->start;
						result += k->start ^ (pivot+6)->start;
						result += k->start ^ (pivot+7)->start;
						result += k->start ^ (pivot+8)->start;
						result += k->start ^ (pivot+9)->start;
						result += k->start ^ (pivot+10)->start;
						result += k->start ^ (pivot+11)->start;
						result += k->start ^ (pivot+12)->start;
						result += k->start ^ (pivot+13)->start;
						result += k->start ^ (pivot+14)->start;
						result += k->start ^ (pivot+15)->start;
#endif
					}
					pivot += 16;
				}
				
				if ((last-pivot >= 8) && (curr->end > (pivot+7)->start))
				{
					for (auto k = curr; k != lastG; k++)
					{
#ifdef WORKLOAD_COUNT
						result += 8;
#else
						result += k->start ^ (pivot+0)->start;
						result += k->start ^ (pivot+1)->start;
						result += k->start ^ (pivot+2)->start;
						result += k->start ^ (pivot+3)->start;
						result += k->start ^ (pivot+4)->start;
						result += k->start ^ (pivot+5)->start;
						result += k->start ^ (pivot+6)->start;
						result += k->start ^ (pivot+7)->start;
#endif
					}
					pivot += 8;
				}
				
				if ((last-pivot >= 4) && (curr->end > (pivot+3)->start))
				{
					for (auto k = curr; k != lastG; k++)
					{
#ifdef WORKLOAD_COUNT
						result += 4;
#else
						result += k->start ^ (pivot+0)->start;
						result += k->start ^ (pivot+1)->start;
						result += k->start ^ (pivot+2)->start;
						result += k->start ^ (pivot+3)->start;
#endif
					}
					pivot += 4;
				}
				
				while ((pivot < last) && (pivot->start < curr->end))
				{
					auto k = curr;
					while (lastG-k >= 32)
					{
#ifdef WORKLOAD_COUNT
						result += 32;
#else
						result += (k+0)->start ^ pivot->start;
						result += (k+1)->start ^ pivot->start;
						result += (k+2)->start ^ pivot->start;
						result += (k+3)->start ^ pivot->start;
						result += (k+4)->start ^ pivot->start;
						result += (k+5)->start ^ pivot->start;
						result += (k+6)->start ^ pivot->start;
						result += (k+7)->start ^ pivot->start;
						result += (k+8)->start ^ pivot->start;
						result += (k+9)->start ^ pivot->start;
						result += (k+10)->start ^ pivot->start;
						result += (k+11)->start ^ pivot->start;
						result += (k+12)->start ^ pivot->start;
						result += (k+13)->start ^ pivot->start;
						result += (k+14)->start ^ pivot->start;
						result += (k+15)->start ^ pivot->start;
						result += (k+16)->start ^ pivot->start;
						result += (k+17)->start ^ pivot->start;
						result += (k+18)->start ^ pivot->start;
						result += (k+19)->start ^ pivot->start;
						result += (k+20)->start ^ pivot->start;
						result += (k+21)->start ^ pivot->start;
						result += (k+22)->start ^ pivot->start;
						result += (k+23)->start ^ pivot->start;
						result += (k+24)->start ^ pivot->start;
						result += (k+25)->start ^ pivot->start;
						result += (k+26)->start ^ pivot->start;
						result += (k+27)->start ^ pivot->start;
						result += (k+28)->start ^ pivot->start;
						result += (k+29)->start ^ pivot->start;
						result += (k+30)->start ^ pivot->start;
						result += (k+31)->start ^ pivot->start;
#endif
						k += 32;
					}
					
					if (lastG-k >= 16)
					{
#ifdef WORKLOAD_COUNT
						result += 16;
#else
						result += (k+0)->start ^ pivot->start;
						result += (k+1)->start ^ pivot->start;
						result += (k+2)->start ^ pivot->start;
						result += (k+3)->start ^ pivot->start;
						result += (k+4)->start ^ pivot->start;
						result += (k+5)->start ^ pivot->start;
						result += (k+6)->start ^ pivot->start;
						result += (k+7)->start ^ pivot->start;
						result += (k+8)->start ^ pivot->start;
						result += (k+9)->start ^ pivot->start;
						result += (k+10)->start ^ pivot->start;
						result += (k+11)->start ^ pivot->start;
						result += (k+12)->start ^ pivot->start;
						result += (k+13)->start ^ pivot->start;
						result += (k+14)->start ^ pivot->start;
						result += (k+15)->start ^ pivot->start;
#endif
						k += 16;
					}
					
					if (lastG-k >= 8)
					{
#ifdef WORKLOAD_COUNT
						result += 8;
#else
						result += (k+0)->start ^ pivot->start;
						result += (k+1)->start ^ pivot->start;
						result += (k+2)->start ^ pivot->start;
						result += (k+3)->start ^ pivot->start;
						result += (k+4)->start ^ pivot->start;
						result += (k+5)->start ^ pivot->start;
						result += (k+6)->start ^ pivot->start;
						result += (k+7)->start ^ pivot->start;
#endif
						k += 8;
					}
					
					if (lastG-k >= 4)
					{
#ifdef WORKLOAD_COUNT
						result += 4;
#else
						result += (k+0)->start ^ pivot->start;
						result += (k+1)->start ^ pivot->start;
						result += (k+2)->start ^ pivot->start;
						result += (k+3)->start ^ pivot->start;
#endif
						k += 4;
					}
					
					while (k != lastG)
					{
#ifdef WORKLOAD_COUNT
						result++;
#else
						result += k->start ^ pivot->start;
#endif
						k++;
					}
					
					pivot++;
				}
				break;
		}
	}
	
	
	return result;
}

//////////////////////////////
// Single-thread processing //
//////////////////////////////

unsigned long long bguFS(Relation &R, Relation &S, BucketIndex &BIR, BucketIndex &BIS)
{
	unsigned long long result = 0;
	auto r = R.begin();
	auto s = S.begin();
	auto lastR = R.end();
	auto lastS = S.end();
	Group GR, GS;
	
	
	while ((r < lastR) && (s < lastS))
	{
		if (*r < *s)
		{
			// Step 1: gather group for R.
			while ((r < lastR) && (r->start < s->start))
			{
				GR.emplace_back(r->start, r->end);
				r++;
			}
			
			// Sort current group by end point.
			sort( GR.begin(), GR.end(), CompareByEnd);
			
			// Step 2: run internal loop.
			result += bguFS_InternalLoop(GR, s, lastS, BIS, S.minStart);


			// Step 3: empty current group.
			GR.clear();
		}
		else
		{
			// Step 1: gather group for S.
			while ((s < lastS) && (r->start >= s->start))
			{
				GS.emplace_back(s->start, s->end);
				s++;
			}
			
			// Sort current group by end point.
			sort( GS.begin(), GS.end(), CompareByEnd );
			
			
			// Step 2: run internal loop.
			result += bguFS_InternalLoop(GS, r, lastR, BIR, R.minStart);

			
			// Step 3: empty current group.
			GS.clear();
		}
	}
	
	
	return result;
}