//
// Created by Ron on 3/20/2021.
//

#include "osm.h"
#include <stdio.h>
#include <sys/time.h>
#define MICRO 1e6
#define MICRO_TO_NANO 1e3
#define UNROLLING_FACTOR 10
#define FAILURE -1
//typedef struct timeval timeval;
//typedef struct timezone timezone;

double calc_timeval_diff(struct timeval tv_start, struct timeval tv_end, size_t iterations) {
    return (((tv_end.tv_sec * MICRO + tv_end.tv_usec)  -
             (tv_start.tv_sec * MICRO + tv_start.tv_usec)) * MICRO_TO_NANO) / (double) iterations;
}

void empty_func() {

}

double osm_operation_time(unsigned int iterations) {
    size_t i;
    struct timeval tv_start;
    struct timeval tv_end;
    int success = gettimeofday(&tv_start, NULL);
    if (iterations == 0 || success == FAILURE)
        return FAILURE;
    for (i=0; i < iterations; i+=UNROLLING_FACTOR) {
        1 + 2;
        3 + 4;
        5 + 6;
        7 + 8;
        9 + 10;
        11 + 12;
        13 + 14;
        15 + 16;
        17 + 18;
        19 + 20;
    }
    success = gettimeofday(&tv_end, NULL);
    if (success == FAILURE)
        return FAILURE;
    return calc_timeval_diff(tv_start, tv_end, i);
}

double osm_function_time(unsigned int iterations) {
    size_t i;
    struct timeval tv_start;
    struct timeval tv_end;
    int success = gettimeofday(&tv_start, NULL);
    if (iterations == 0 || success == FAILURE)
        return FAILURE;
    for (i=0; i < iterations; i+=UNROLLING_FACTOR) {
        empty_func();
        empty_func();
        empty_func();
        empty_func();
        empty_func();
        empty_func();
        empty_func();
        empty_func();
        empty_func();
        empty_func();
    }
    success = gettimeofday(&tv_end, NULL);
    if (success == FAILURE)
        return FAILURE;
    return calc_timeval_diff(tv_start, tv_end, i);

}

double osm_syscall_time(unsigned int iterations) {
    size_t i;
    struct timeval tv_start;
    struct timeval tv_end;
    int success = gettimeofday(&tv_start, NULL);
    if (iterations == 0 || success == FAILURE)
        return FAILURE;
    for (i=0; i < iterations; i+=UNROLLING_FACTOR)
    {
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
    }
    success = gettimeofday(&tv_end, NULL);
    if (success == FAILURE)
        return FAILURE;
    return calc_timeval_diff(tv_start, tv_end, i);
}
