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


#include "relation.h"

BordersElement::BordersElement()
{
}

BordersElement::BordersElement(uint32_t group1, uint32_t group2, uint32_t position_start, uint32_t position_end)
{
    this->group1 = group1;
    this->group2 = group2;
    this->position_start = position_start;
    this->position_end = position_end;
}

BordersElement::~BordersElement()
{
}

ExtendedRecord::ExtendedRecord()
{
}

//ExtendedRecord::ExtendedRecord(RecordId id, Timestamp start, Timestamp end, long int group)
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

ExtendedRelation::ExtendedRelation()
{
	this->minStart = numeric_limits<Timestamp>::max();
	this->maxEnd   = numeric_limits<Timestamp>::min();
}

void ExtendedRelation::load(const char *filename)
{
	Timestamp start, end;
	uint32_t group1, group2;
	ifstream inp(filename);

	if (!inp)
	{
		cerr << "error - cannot open file " << filename << endl;
		exit(1);
	}

	while (inp >> start >> end >> group1 >> group2)
	{
		this->emplace_back(start, end, group1, group2);

		this->minStart = min(this->minStart, start);
		this->maxEnd   = max(this->maxEnd  , end);
	}
	inp.close();
}

/* for a range of [1,size], sets toTakeStart and toTakeEnd with equal subrange to be read, for thread with id chunk out of c total threads */
void ExtendedRelation::chunk_to_read(uint32_t size, uint32_t c, uint32_t chunk, uint32_t &toTakeStart, uint32_t &toTakeEnd)
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

void ExtendedRelation::find_borders(structForParallelComplement *gained)
{
    if (this->size() >= gained->c)
    {
        // find chunk to read from S
        uint32_t toTakeStart, toTakeEnd;
        this->chunk_to_read( this->size(), gained->c, gained->chunk, toTakeStart, toTakeEnd);

        --toTakeStart;
        --toTakeEnd;

        // find borders between [toTakeStart,toTakeEnd]
        Timestamp last = toTakeStart;
        for (uint32_t i = toTakeStart; i < toTakeEnd; i++)
        {
            if ( ((*this)[i].group1 != (*this)[i+1].group1) || ((*this)[i].group2 != (*this)[i+1].group2) )
            {
                this->localBorders[gained->chunk].push_back( BordersElement( (*this)[last].group1, (*this)[last].group2, last, i) );
                last = i + 1;
            }
        }
        this->localBorders[gained->chunk].push_back( BordersElement( (*this)[last].group1, (*this)[last].group2, last, toTakeEnd) );
    }
    else if (gained->chunk == 0)
    {
        Timestamp last = 0;
        for (uint32_t i=0; i < (this->size()-1); i++)
        {
            if ( ((*this)[i].group1 != (*this)[i+1].group1) || ((*this)[i].group2 != (*this)[i+1].group2) )
            {
                this->localBorders[gained->chunk].push_back( BordersElement( (*this)[last].group1, (*this)[last].group2, last, i) );
                last = i + 1;
            }
        }
        this->localBorders[gained->chunk].push_back( BordersElement( (*this)[last].group1, (*this)[last].group2, last, this->size()-1) );
    }
}

void mainBorders( ExtendedRelation& R, ExtendedRelation& S, uint32_t c)
{
    #ifdef TIMES
    Timer tim;
    tim.start();
    #endif

    // find borders of each group in sorted R
    R.localBorders.resize(c);
    structForParallelComplement toPass[c];
    pthread_t threads[c];
    for (long int i=0; i < c; i++)
    {
        toPass[i].rel = &R;
        toPass[i].c = c;
        toPass[i].chunk = i;
        pthread_create( &threads[i], NULL, &ExtendedRelation::find_borders_helper, &toPass[i]);
    }
    for (long int i=0; i < c; i++)
    {
        pthread_join( threads[i], NULL);
    }
    R.borders.insert( R.borders.end(), R.localBorders[0].begin(), R.localBorders[0].end() );
    if(R.size() >= c)
    {
        for (uint32_t i=1,j; i < c; i++)
        {
            j = 0;
            if ( (R.localBorders[i][j].group1 == (R.borders.end()-1)->group1) && (R.localBorders[i][j].group2 == (R.borders.end()-1)->group2) )
            {
                (R.borders.end()-1)->position_end = R.localBorders[i][j].position_end;
                j = 1;
            }
            R.borders.insert( R.borders.end(), R.localBorders[i].begin() + j, R.localBorders[i].end() );
        }
    }

    // find borders of each group in sorted S
    S.localBorders.resize(c);
    for (long int i=0; i < c; i++)
    {
        toPass[i].rel = &S;
        toPass[i].c = c;
        toPass[i].chunk = i;
        pthread_create( &threads[i], NULL, &ExtendedRelation::find_borders_helper, &toPass[i]);
    }
    for (long int i=0; i < c; i++)
    {
        pthread_join( threads[i], NULL);
    }
    S.borders.insert( S.borders.end(), S.localBorders[0].begin(), S.localBorders[0].end() );
    if(S.size() >= c)
    {
        for (uint32_t i=1,j; i < c; i++)
        {
            j = 0;
            if ( (S.localBorders[i][j].group1 == (S.borders.end()-1)->group1) && (S.localBorders[i][j].group2 == (S.borders.end()-1)->group2) )
            {
                (S.borders.end()-1)->position_end = S.localBorders[i][j].position_end;
                j = 1;
            }
            S.borders.insert( S.borders.end(), S.localBorders[i].begin() + j, S.localBorders[i].end() );
        }
    }
    S.localBorders.clear();

    #ifdef TIMES
    double timeFindBorders = tim.stop();
    cout << "FindBorders time: " << timeFindBorders << endl;
    #endif
}

void ExtendedRelation::find_sizes(structForParallelComplement *gained)
{
    uint32_t count=0;
    Timestamp last = this->domainStart;

    // find size of current group
    for (uint32_t i = this->borders[gained->groups_index].position_start; i <= this->borders[gained->groups_index].position_end; i++)
    {
        if (last < (*this)[i].start)
        {
            last = (*this)[i].end;
            count++;
        }
        else if (last < (*this)[i].end)
        {
            last = (*this)[i].end;
        }
    }
    if (last < this->domainEnd)
    {
        count++;
    }

    // set size of current thread in the current group
    this->each_group_sizes[gained->groups_index] = count;

    // make current thread free to be used for next group
    this->jobsList[gained->chunk] = 2;
}

void ExtendedRelation::set_complement(structForParallelComplement *gained)
{
    uint32_t point_to_write = each_group_sizes[gained->groups_index];
    Timestamp last = this->domainStart;
    bool write_flag = false;

    // set complement
    for (uint32_t i = this->borders[gained->groups_index].position_start; i <= this->borders[gained->groups_index].position_end; i++)
    {
        if (last < (*this)[i].start)
        {
            write_flag = true;
            this->complementS[point_to_write] = ExtendedRecord(last, (*this)[i].start, gained->groups_name.first, gained->groups_name.second);
            last = (*this)[i].end;
            point_to_write++;
        }
        else if (last < (*this)[i].end)
        {
            last = (*this)[i].end;
        }
    }
    if (last < this->domainEnd)
    {
        write_flag = true;
        this->complementS[point_to_write] = ExtendedRecord(last, this->domainEnd, gained->groups_name.first, gained->groups_name.second);
        point_to_write++;
    }

    // set borders for current group
    if (write_flag)
    {
        this->borders[gained->groups_index].position_start = each_group_sizes[gained->groups_index];
        this->borders[gained->groups_index].position_end = point_to_write - 1;
    }
    else
    {
        this->borders[gained->groups_index].position_start = 1;
        this->borders[gained->groups_index].position_end = 0;
    }

    // make current thread free to be used for next group
    this->jobsList[gained->chunk] = 2;
}

long int ExtendedRelation::getThreadId(bool& needsDetach)
{
    uint32_t i=0;
    while(true)
    {
        if (this->jobsList[i] != 0)
        {
            if (this->jobsList[i] == 2)
                needsDetach = true;
            this->jobsList[i] = 0;
            break;
        }

        i == (this->jobsList.size() - 1) ? i = 0: i++;
    }

    return i;
}

void ExtendedRelation::complement(long int c, Timestamp foreignStart, Timestamp foreignEnd)
{
    structForParallelComplement toPass[c];
    pthread_t threads[c];

    // set domain end
    this->domainStart = min( foreignStart, this->minStart);
    this->domainEnd = max( foreignEnd, this->maxEnd);

    ///////////////////////////////// find the size of complementS /////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////

    // initialize each_group_sizes[this->borders.size()][c]
    uint32_t groups_num = this->borders.size();
    this->each_group_sizes.resize(groups_num, 0);

    this->jobsList.resize( c, 1);
    uint32_t current_groups_index = 0;
    uint32_t threadId;
    bool needsDetach;
    for (auto& b : this->borders)
    {
        needsDetach = false;
        threadId = this->getThreadId(needsDetach);
        if (needsDetach)
            if (pthread_detach(threads[threadId]))
                printf("Whoops");

        toPass[threadId].rel = this;
        toPass[threadId].c = c;
        toPass[threadId].chunk = threadId;
        toPass[threadId].groups_index = current_groups_index;
        toPass[threadId].groups_name = pair<uint32_t,uint32_t>(b.group1,b.group2);
        pthread_create( &threads[threadId], NULL, &ExtendedRelation::find_sizes_helper, &toPass[threadId]);

        // next group
        current_groups_index++;

    }
    for (uint32_t i=0; i < c; i++)
    {
        if (this->jobsList[i] != 1)
            pthread_join( threads[i], NULL);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////// set complementS ////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////

    // calculate full size of complementS, change group sizes to points that each group should begin at new table
    uint32_t total = 0, previous_total;
    for (uint32_t i=0; i < groups_num; i++)
    {
        previous_total = total;
        total += each_group_sizes[i];
        each_group_sizes[i] = previous_total;
    }

    this->complementS.resize(total);

    fill( this->jobsList.begin(), this->jobsList.end(), true);
    current_groups_index = 0;
    for (auto& b : this->borders)
    {
        needsDetach = false;
        threadId = this->getThreadId(needsDetach);
        if (needsDetach)
            if (pthread_detach(threads[threadId]))
                printf("Whoops");

        toPass[threadId].rel = this;
        toPass[threadId].c = c;
        toPass[threadId].chunk = threadId;
        toPass[threadId].groups_index = current_groups_index;
        toPass[threadId].groups_name = pair<uint32_t,uint32_t>(b.group1,b.group2);
        pthread_create( &threads[threadId], NULL, &ExtendedRelation::set_complement_helper, &toPass[threadId]);

        // next group
        current_groups_index++;
    }
    for (uint32_t i=0; i < c; i++)
    {
        if (this->jobsList[i] != 1)
            pthread_join( threads[i], NULL);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    // move complement to main relation
    std::swap( *this, this->complementS);

    // remove memory for data structures you don't need anymore
    this->complementS.clear();
    this->each_group_sizes.clear();
    this->jobsList.clear();
/*
    cout << endl << "Complement of S:";
    for (uint32_t i=0; i < total; i++)
        cout << "{[" << (*this)[i].start << "," << (*this)[i].end << ")," << (*this)[i].group << "}  ";
    cout << endl << endl;
*/
}

ExtendedRelation::~ExtendedRelation()
{
}


bool CompareByEnd(const Record& lhs, const Record& rhs)
{
	return (lhs.end < rhs.end);
}



Record::Record()
{
}


//Record::Record(RecordId id, Timestamp start, Timestamp end)
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


void Record::print(char c) const
{
	cout << c << "[" << this->start << ".." << this->end << "]" << endl;
}

   
Record::~Record()
{
}



Relation::Relation()
{
	this->minStart = numeric_limits<Timestamp>::max();
	this->maxStart = numeric_limits<Timestamp>::min();
	this->minEnd   = numeric_limits<Timestamp>::max();
	this->maxEnd   = numeric_limits<Timestamp>::min();
	this->longestRecord = numeric_limits<Timestamp>::min();
}


void Relation::load(const char *filename)
{
	Timestamp start, end;
	uint32_t group;
	ifstream inp(filename);

	
	if (!inp)
	{
		cerr << "error - cannot open file " << filename << endl;
		exit(1);
	}

	while (inp >> start >> end)
	{
		this->emplace_back(start, end);

		this->minStart = min(this->minStart, start);
		this->maxStart = max(this->maxStart, start);
		this->minEnd   = min(this->minEnd  , end);
		this->maxEnd   = max(this->maxEnd  , end);
		this->longestRecord = max(this->longestRecord, end-start+1);
	}
	inp.close();
	
	this->numRecords = this->size();
}


void Relation::load(const Relation& I, size_t from, size_t by)
{
	size_t tupleCount = (I.size() + by - 1 - from) / by;


	for (size_t i = from; i < I.size(); i += by)
	{
		this->emplace_back(I[i].start, I[i].end);

		this->minStart = min(this->minStart, I[i].start);
		this->maxStart = max(this->maxStart, I[i].start);
		this->minEnd   = min(this->minEnd  , I[i].end);
		this->maxEnd   = max(this->maxEnd  , I[i].end);
		this->longestRecord = max(this->longestRecord, I[i].end-I[i].start+1);
	}
    this->numRecords = by - from;

	sort(this->begin(), this->end());
}

void Relation::load(const ExtendedRelation& I, size_t from, size_t till)
{
    for (size_t i = from; i <= till; i++)
    {
        this->emplace_back(I[i].start, I[i].end);

        this->minStart = min(this->minStart, I[i].start);
        this->maxStart = max(this->maxStart, I[i].start);
        this->minEnd   = min(this->minEnd  , I[i].end);
        this->maxEnd   = max(this->maxEnd  , I[i].end);
        this->longestRecord = max(this->longestRecord, I[i].end-I[i].start+1);
    }

    this->numRecords = this->size();
}

void Relation::sortByStart()
{
	sort(this->begin(), this->end());
}


void Relation::sortByEnd()
{
	sort(this->begin(), this->end(), CompareByEnd);
}


void Relation::print(char c)
{
	for (const Record& rec : (*this))
		rec.print(c);
}


Relation::~Relation()
{
}
