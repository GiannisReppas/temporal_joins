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

#include "relation.hpp"

/**************************************************************************************************/

ExtendedRecord::ExtendedRecord()
{
}

ExtendedRecord::ExtendedRecord(Timestamp start, Timestamp end, uint32_t group1, uint32_t group2)
{
	this->start = start;
	this->end = end;
	this->group1 = group1;
	this->group2 = group2;
}

ExtendedRecord::~ExtendedRecord()
{
}

/**************************************************************************************************/

ExtendedRelation::ExtendedRelation()
{
	this->record_list = NULL;

	this->minStart = std::numeric_limits<Timestamp>::max();
	this->maxEnd   = std::numeric_limits<Timestamp>::min();

	this->numRecords = 0;
}

void ExtendedRelation::load(const char *filename)
{
	Timestamp start, end;
	uint32_t group1, group2;
	std::ifstream inp(filename);
	if (!inp)
	{
		std::cout << "error - cannot open file " << filename << std::endl;
		exit(1);
	}

	while (inp >> start >> end >> group1 >> group2)
	{
		this->numRecords++;
	}
	this->record_list = (ExtendedRecord*) malloc(sizeof(ExtendedRecord)*this->numRecords);
	inp.close();

	inp.open(filename);
	size_t i = 0;
	while (i < this->numRecords)
	{
		inp >> start >> end >> group1 >> group2;

		this->record_list[i] = ExtendedRecord(start, end, group1, group2);

		this->minStart = std::min(this->minStart, start);
		this->maxEnd   = std::max(this->maxEnd  , end);

		i++;
	}
	inp.close();
}

ExtendedRelation::~ExtendedRelation()
{
	free( this->record_list );
}

/**************************************************************************************************/

Record::Record()
{
}

Record::Record(Timestamp start, Timestamp end)
{
	this->start = start;
	this->end = end;
}

bool Record::operator < (const Record& rhs) const
{
	return this->start < rhs.start;
}

bool Record::operator >= (const Record& rhs) const
{
	return !((*this) < rhs);
}

Record::~Record()
{
}

/**************************************************************************************************/

Relation::Relation()
{
	this->minStart = std::numeric_limits<Timestamp>::max();
	this->maxStart = std::numeric_limits<Timestamp>::min();
	this->minEnd   = std::numeric_limits<Timestamp>::max();
	this->maxEnd   = std::numeric_limits<Timestamp>::min();

	this->record_list = NULL;
	this->numRecords = 0;
}

void Relation::load(const ExtendedRelation& I, size_t from, size_t till)
{
	this->record_list = (Record*) malloc( (till - from + 1) * sizeof(Record) );

	for (size_t i = from; i <= till; i++)
	{
		this->record_list[i - from] = Record(I.record_list[i].start, I.record_list[i].end);

		this->minStart = std::min(this->minStart, I.record_list[i].start);
		this->maxStart = std::max(this->maxStart, I.record_list[i].start);
		this->minEnd   = std::min(this->minEnd  , I.record_list[i].end);
		this->maxEnd   = std::max(this->maxEnd  , I.record_list[i].end);
	}

	this->numRecords = till - from + 1;
}

Relation::~Relation()
{
	free( this->record_list );
}
