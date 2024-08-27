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


#include "bucket_index.hpp"



Bucket::Bucket()
{
}


Bucket::Bucket(Record* i)
{
	this->last = i;
}


Bucket::~Bucket()
{
}



BucketIndex::BucketIndex()
{
	this->bucket_list = NULL;
}


void BucketIndex::build(const Relation &R, long int numBuckets)
{
	long int cbucket_id = 0, btmp;
	Record* i = R.record_list;
	Record* lastI = R.record_list + R.numRecords;
	auto ms = R.maxStart;
	
	if (R.minStart == R.maxStart)
		ms += 1;
	
	this->numBuckets = numBuckets;
	this->bucket_range = (Timestamp)ceil((double)(ms-R.minStart)/this->numBuckets);
	this->bucket_list = (Bucket*) malloc( numBuckets * sizeof(Bucket) );
	for (long int i = 0; i < this->numBuckets; i++)
		this->bucket_list[i] = Bucket(lastI);

	while (i != lastI)
	{
		btmp = ceil((double)(i->start-R.minStart)/this->bucket_range);
		if (btmp >= this->numBuckets)
			btmp = this->numBuckets-1;
		
		if (cbucket_id != btmp)
		{
			this->bucket_list[cbucket_id].last = i;
			cbucket_id++;
		}
		else
		{
			++i;
		}
	}
	this->bucket_list[this->numBuckets-1].last = lastI;
	for (long int i = this->numBuckets-2; i >= 0; i--)
	{
		if (this->bucket_list[i].last == lastI)
			this->bucket_list[i].last = this->bucket_list[i+1].last;
	}
}

BucketIndex::~BucketIndex()
{
	free( this->bucket_list );
}
