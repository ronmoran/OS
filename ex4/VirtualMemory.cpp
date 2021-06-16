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


static void translateAddr(uint64_t virtualAddress, uint64_t *buffer) {
    int i = 0;
    while (i < TABLES_DEPTH) {
        buffer[i++] = virtualAddress & ((1 << OFFSET_WIDTH) - 1);
        virtualAddress = virtualAddress >> OFFSET_WIDTH;
    }
}

static word_t findFree(word_t address, int *depth, word_t *free) {
    int freeCells = 0;
    int value = 0;
    if (*depth > TABLES_DEPTH) {
        return -1;
    }
    for (int off = 0; off < OFFSET_WIDTH; ++off) {
        PMread(address + off, &value);
        if (value == 0) {
            freeCells++;
        } else {
            if (*free == -1){
                findFree(value + off, depth + 1, free);
            }
        }
    }
    if (freeCells == OFFSET_WIDTH) {
        *free = address;
    }
}


static void evict(uint64_t address, int *currMax) {
//    todo

}

int VMread(uint64_t virtualAddress, word_t *value) {

    return 1;
}


int VMwrite(uint64_t virtualAddress, word_t value) {
    uint64_t pagesOffsets[TABLES_DEPTH];
    translateAddr(virtualAddress, pagesOffsets);
    word_t currAddress = 0;
    word_t prevAddress = 0;
    int64_t pageOff;
    for (int j=0; j<TABLES_DEPTH;j++) {
        pageOff = pagesOffsets[j];
        PMread(prevAddress * PAGE_SIZE + pageOff, &currAddress);
        if(currAddress == 0){
            int depth = 0;
            word_t freeFrame = -1;
            findFree(0, &depth, &freeFrame);
            if (freeFrame == -1) { //need to evict
                std::cout << "evict " << std::endl;
                for (int i = 0; i < OFFSET_WIDTH; i++) {
                    clearTable(freeFrame);
                }
//            evict(virtualAddress, &currAddress);
            }
            word_t val = freeFrame+1;
            if (j == TABLES_DEPTH -1){
                val = value;
            }
            PMwrite(prevAddress + pageOff, val);
            prevAddress = freeFrame+1;
        }
    }
    return 1;
}



int main(int argc, char **argv) {
    VMinitialize();
//    uint64_t myArr[TABLES_DEPTH];
//    translateAddr(65531, myArr);

    for (uint64_t i = 0; i < (2 * NUM_FRAMES); ++i) {
        printf("writing to %llu\n", (long long int) i);
        VMwrite(5 * i * PAGE_SIZE, i);
    }
}
