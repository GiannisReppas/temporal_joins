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

#ifndef _RELATION_H_
#define _RELATION_H_

#include "../def.h"

class ExtendedRelation;

class BordersElement
{
public:
    uint32_t group1;
    uint32_t group2;
    uint32_t position_start;
    uint32_t position_end;

    BordersElement();
    BordersElement(uint32_t group1, uint32_t group2, uint32_t position_start, uint32_t position_end);
    ~BordersElement();
};

struct structForParallelComplement
{
    ExtendedRelation *rel;

    uint32_t c; // number of threads
    uint32_t chunk; // thread id [0,c)
    uint32_t groups_index; // index of current group [0,groups_num-1]
    pair<uint32_t,uint32_t> groups_name; // name of group
};

struct structForParallel_bgFS
{
    int threadId;
    int runNumBuckets;
    ExtendedRelation* exR;
    ExtendedRelation* exS;
    uint32_t R_start;
    uint32_t R_end;
    uint32_t S_start;
    uint32_t S_end;
};

class ExtendedRecord
{
public:
    Timestamp start;
    Timestamp end;
    uint32_t group1;
    uint32_t group2;

    ExtendedRecord();
    ExtendedRecord(Timestamp start, Timestamp end, uint32_t group1, uint32_t group2);
    ~ExtendedRecord();
};

class ExtendedRelation : public vector<ExtendedRecord>
{
private:
    /* for a range of [1,size], sets toTakeStart and toTakeEnd with subrange to be read, for thread with id chunk out of c total threads */
    void chunk_to_read(uint32_t size, uint32_t c, uint32_t chunk, uint32_t &toTakeStart, uint32_t &toTakeEnd);

    /* domain (smallest startpoint and biggest endpoint from both exR and exS) */
    Timestamp domainStart;
    Timestamp domainEnd;

    /* job lists that shows ids of unused threads */
    vector<long int> jobsList;
    long int getThreadId(bool&);

    /* used to keep the #tuples of each group in complementS */
    vector<size_t> each_group_sizes;

    /* stores result relation complementS (until the end of complement function) */
    vector<ExtendedRecord> complementS;

    /* parallel section to set the group locations after we sort S */
    void find_borders(structForParallelComplement*);

    /* parallel sections to find the size and the values of complementS */
    void find_sizes(structForParallelComplement*);
    static void *find_sizes_helper(void* args)
    {
        structForParallelComplement *gained = (structForParallelComplement*) args;

        gained->rel->find_sizes(gained);

        return NULL;
    }
    void set_complement(structForParallelComplement*);
    static void *set_complement_helper(void* args)
    {
        structForParallelComplement *gained = (structForParallelComplement*) args;

        gained->rel->set_complement(gained);

        return NULL;
    }
public:
    Timestamp minStart, maxEnd;

    /* used to find borders in a single parallel execution (subpart of Extended Relation) */
    vector< vector< BordersElement > > localBorders;
    static void *find_borders_helper(void* args)
    {
        structForParallelComplement *gained = (structForParallelComplement*) args;

        gained->rel->find_borders(gained);

        return NULL;
    }

    vector< BordersElement > borders; // shows parts in S that a new group starts and ends

    ExtendedRelation();
    void load(const char *filename);
    void complement(long int c, Timestamp foreignStart, Timestamp foreignEnd); // changes Extended Relation to its complement
    ~ExtendedRelation();
};

void mainBorders( ExtendedRelation& R, ExtendedRelation& S, uint32_t c);

class Record
{
public:
	Timestamp start;
	Timestamp end;

	Record();
	Record(Timestamp start, Timestamp end);
	bool operator < (const Record& rhs) const;
	bool operator >= (const Record& rhs) const;
	void print() const;
	void print(char c) const;
	~Record();
};



class Relation : public vector<Record>
{
public:
	size_t numRecords;
	Timestamp minStart, maxStart, minEnd, maxEnd;
	Timestamp longestRecord;

	Relation();
	void load(const char *filename);
	void load(const Relation& I, size_t from = 0, size_t by = 1);
    void load(const ExtendedRelation& I, size_t from, size_t till);
	void sortByStart();
	void sortByEnd();
	void print(char c);
	~Relation();
};
typedef Relation::const_iterator RelationIterator;

typedef Relation Group;
typedef Group::const_iterator GroupIterator;
#endif //_RELATION_H_
