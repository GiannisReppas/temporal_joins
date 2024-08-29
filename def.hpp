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

#pragma once
#ifndef _DEF_H_
#define _DEF_H_

#include <iostream>
#include <limits>
#include <vector>
#include <algorithm>
#include <string>
#include <fstream>
#include <chrono>
#include <cmath>
#include <unistd.h>
#include <pthread.h>
#include <cstring>
#include <map>

/* LOGGING PARAMETERS */
//#define TIMES
#define WORKLOAD_COUNT

/* JOIN TYPES */
#define INNER_JOIN 0
#define LEFT_OUTER_JOIN 1
#define RIGHT_OUTER_JOIN 2
#define FULL_OUTER_JOIN 3
#define ANTI_JOIN 4

/* AVAILABLE STRATEGIES TO RUN (ANTI) JOIN */
#define BGU_FS 0
#define DIP 1

typedef unsigned long long Timestamp;

class Timer
{
private:
	using Clock = std::chrono::high_resolution_clock;
	Clock::time_point start_time, stop_time;

public:
	Timer()
	{
		start();
	}

	void start()
	{
		start_time = Clock::now();
	}

	double getElapsedTimeInSeconds()
	{
		return std::chrono::duration<double>(stop_time - start_time).count();
	}

	double stop()
	{
		stop_time = Clock::now();
		return getElapsedTimeInSeconds();
	}
};
#endif
