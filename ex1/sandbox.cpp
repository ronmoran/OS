//
// Created by Ron on 3/20/2021.
//
#include "osm.cpp"
int main() {
    printf("Time for addition %.2f nanoseconds", osm_operation_time(105));
    printf("\n");
    printf("Time for function %.2f nanoseconds", osm_function_time(105));
    printf("\n");
    printf("Time for syscall %.2f nanoseconds", osm_syscall_time(105));
    return 0;
}
