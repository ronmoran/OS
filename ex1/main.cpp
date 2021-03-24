//
// Created by Ron on 3/20/2021.
//
#include "osm.cpp"
#include <stdlib.h>
int main(int argc, char* argv[]) {
    int iterations = strtol(argv[1], NULL, 10);
    printf("Time for addition %.2f nanoseconds\n", osm_operation_time(iterations));
    printf("Time for function %.2f nanoseconds\n", osm_function_time(iterations));
    printf("Time for syscall %.2f nanoseconds\n", osm_syscall_time(iterations));
    return 0;
}
