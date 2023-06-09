#include "osm.h"
#include <stdio.h>
#include <sys/time.h>
#define MICRO 1e6
#define MICRO_TO_NANO 1e3
#define UNROLLING_FACTOR 10
#define FAILURE -1

double calc_timeval_diff(struct timeval tv_start, struct timeval tv_end, size_t iterations) {
    return (((tv_end.tv_sec * MICRO + tv_end.tv_usec)  -
             (tv_start.tv_sec * MICRO + tv_start.tv_usec)) * MICRO_TO_NANO) / (double) iterations;
}

void emptyFunc() {

}

double osm_operation_time(unsigned int iterations) {
    size_t i;
    struct timeval tv_start;
    struct timeval tv_end;
    int success = gettimeofday(&tv_start, NULL);
    if (iterations == 0 || success == FAILURE)
        return FAILURE;
    int x=0; // without this line the compiler just computes the additions
    for (i=0; i < iterations; i+=UNROLLING_FACTOR) {
        x=x + 1;
        x=x + 4;
        x=x + 6;
        x=x + 8;
        x=x + 10;
        x=x + 12;
        x=x + 14;
        x=x + 16;
        x=x + 18;
        x=x + 20;
    }
    success = gettimeofday(&tv_end, NULL);
    (void)x; //suppress warning
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
        emptyFunc();
        emptyFunc();
        emptyFunc();
        emptyFunc();
        emptyFunc();
        emptyFunc();
        emptyFunc();
        emptyFunc();
        emptyFunc();
        emptyFunc();
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
