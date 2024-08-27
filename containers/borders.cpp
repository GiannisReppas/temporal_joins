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

#include "borders.hpp"

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

/**************************************************************************************************/

Borders::Borders()
{
	this->borders_list = NULL;
	this->numBorders = 0;
}

Borders::~Borders()
{
	free( this->borders_list );
}