//
// Created by Ron on 3/20/2021.
//
#include "osm.cpp"
#include <stdlib.h>
int main(int argc, char* argv[]) {
    int iterations = strtol(argv[1], NULL, 10);
    printf("Time for addition %.2f nanoseconds", osm_operation_time(iterations));
    printf("\n");
    printf("Time for function %.2f nanoseconds", osm_function_time(iterations));
    printf("\n");
    printf("Time for syscall %.2f nanoseconds", osm_syscall_time(iterations));
    return 0;
}
