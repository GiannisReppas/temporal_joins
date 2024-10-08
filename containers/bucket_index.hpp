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


#ifndef _BUCKET_INDEX_H_
#define _BUCKET_INDEX_H_

#include "../def.hpp"
#include "relation.hpp"

class Bucket
{
public:
	Record* last;

	Bucket();
	Bucket(Record* i);
	~Bucket();
};

class BucketIndex
{
public:
	Bucket* bucket_list;
	long int numBuckets;
	Timestamp bucket_range;
	
	BucketIndex();
	void build(const Relation &R, long int numBuckets);
	~BucketIndex();
};

#endif //_BUCKET_INDEX_H_
