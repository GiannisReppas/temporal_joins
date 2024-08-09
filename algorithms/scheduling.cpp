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

#include "../def.hpp"

/*
Returns a thread id that is free to be used.
Array jobs contains all available threads.
Each thread has 3 different states:
	0 --> thread is currently executing a job and its unavailable 
	1 --> thread is available and doesn't need to be detached to be used again
	2 --> thread is available and needs to be detached to be used again
boolean varriable needsDetach has to be used to inform the caller about the need to be detached
*/
uint32_t getThreadId(bool& needsDetach, std::vector<uint32_t>& jobs)
{
	uint32_t i=0;
	while(true)
	{
		if (jobs[i] != 0)
		{
			if (jobs[i] == 2)
				needsDetach = true;
			jobs[i] = 0;
			break;
		}

		i == (jobs.size() - 1) ? i = 0: i++;
	}

	return i;
}