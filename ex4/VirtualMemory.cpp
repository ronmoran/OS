#include <iostream>
#include "VirtualMemory.h"
#include "PhysicalMemory.h"

#define CEIL(VARIABLE) ( (VARIABLE - (int)VARIABLE)==0 ? (int)VARIABLE : (int)VARIABLE+1 )

void clearTable(uint64_t frameIndex) {
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
        PMwrite(frameIndex * PAGE_SIZE + i, 0);
    }
}

void VMinitialize() {
    clearTable(0);
}


int VMread(uint64_t virtualAddress, word_t* value) {
    uint64_t myArr[TABLES_DEPTH];

    return 1;
}


int VMwrite(uint64_t virtualAddress, word_t value) {
    return 1;
}

static void translateAddr(uint64_t virtualAddress, uint64_t* buffer)
{
    int i = 0;
    while(virtualAddress > 0)
    {
        buffer[i++] = virtualAddress & ((1 <<OFFSET_WIDTH) - 1);
        virtualAddress = virtualAddress >> OFFSET_WIDTH;
    }
}

int main(int argc, char **argv)
{
    VMinitialize();
    uint64_t myArr[TABLES_DEPTH];
    translateAddr(65531, myArr);
    for (unsigned long i : myArr)
    {
        std::cout << i << std::endl;
    }
}
