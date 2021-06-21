#include <iostream>
#include "VirtualMemory.h"
#include "PhysicalMemory.h"

//#define CEIL(VARIABLE) ( (VARIABLE - (int)VARIABLE)==0 ? (int)VARIABLE : (int)VARIABLE+1 )


int getPhysicalAddress(uint64_t virtualAddress, word_t value);

void clearTable(uint64_t frameIndex)
{
    for (uint64_t i = 0; i < PAGE_SIZE; ++i)
    {
        PMwrite(frameIndex * PAGE_SIZE + i, 0);
    }
}

void VMinitialize()
{
    clearTable(0);
}


static void translateAddr(uint64_t virtualAddress, uint64_t *buffer)
{
    int i = 0;
    while (i < TABLES_DEPTH+1)
    {
        buffer[i++] = virtualAddress & ((1 << OFFSET_WIDTH) - 1);
        virtualAddress = virtualAddress >> OFFSET_WIDTH;
    }
}


static void
evict(uint64_t address, int *dataPageAddress, uint64_t *pageVAddr,
      uint64_t *evictParent, uint64_t accumOff, uint64_t parentAddr, int currentWeight, int depth,
      int *maxWeight)
{
    word_t value = 0;
    if (depth < TABLES_DEPTH)
    {
        for (int off = 0; off < PAGE_SIZE; ++off)
        {
            PMread(address*PAGE_SIZE + off, &value);
            if (value != 0)
            {
                int newWeight = currentWeight + (value % 2 == 0 ? WEIGHT_EVEN : WEIGHT_ODD);

                evict(value, dataPageAddress, evictParent, pageVAddr,
                      depth == TABLES_DEPTH - 1? (accumOff + off):(accumOff + off) << OFFSET_WIDTH, address, newWeight,
                      depth + 1, maxWeight); //todo no trenary
            }
        }
    } else if (depth == TABLES_DEPTH)
    {
        currentWeight += (accumOff % 2 == 0 ? WEIGHT_EVEN : WEIGHT_ODD);
        if (currentWeight > *maxWeight || (currentWeight == *maxWeight && accumOff < *pageVAddr))
        {
            *maxWeight = currentWeight;
            *dataPageAddress = address;
            *evictParent = parentAddr;
            *pageVAddr = accumOff;
        }
    }


    if (depth == 0)
    {
        PMevict(*dataPageAddress, *pageVAddr);
        clearTable(*dataPageAddress);
        PMwrite(*evictParent * PAGE_SIZE + (((1ULL << OFFSET_WIDTH) - 1) & *pageVAddr), 0);
    }
}


static void findFree(uint64_t address, int depth, int *free, uint64_t parentNode, uint64_t childOffset,
                     uint64_t selfNode, uint64_t *maxFrame)
{
    int freeCells = 0;
    int value = 0;
    if (depth > TABLES_DEPTH)
    {
        return;
    }
    *maxFrame = address > *maxFrame ? address : *maxFrame;
    for (int off = 0; off < PAGE_SIZE; ++off)
    {
        PMread(address* PAGE_SIZE + off, &value);
        if (value == 0)
        {
            freeCells++;
        } else
        {
            if (*free == -1)
            {
                findFree(value, depth + 1, free, address, off, selfNode, maxFrame);
            } else
            {
                break;
            }
        }
    }
    if (freeCells == PAGE_SIZE && address != selfNode && depth < TABLES_DEPTH)
    { //case 1=
        PMwrite(parentNode*PAGE_SIZE + childOffset, 0); // reset node
        *free = address;
    }  else if (depth == 0 && *free == -1) //2, 3
    { //finished recursion w/o finding a free frame

        if (*maxFrame < NUM_FRAMES - 1)
        { //case 2
            *free = *maxFrame + 1;
            clearTable(*free);
        } else
        {
            uint64_t evictOffset = 0;
            uint64_t evictParent = 0;
            int maxWeight = 0;
            evict(0, free, &evictOffset, &evictParent, 0, 0, 0, 0, &maxWeight); //case 3
        }
    }
}


int getPhysicalAddress(uint64_t virtualAddress, word_t value)
{
    uint64_t pagesOffsets[TABLES_DEPTH+1];
    translateAddr(virtualAddress, pagesOffsets);
    word_t currAddress = 0;
    word_t prevAddress = 0;
    int64_t pageOff;
    for (int j = 0; j <= TABLES_DEPTH; j++)
    {
        pageOff = pagesOffsets[TABLES_DEPTH-j];
        PMread(prevAddress * PAGE_SIZE + pageOff, &currAddress);
        if (currAddress == 0)
        {
            int depth = 0;
            int freeFrame = -1;
            uint64_t maxFrame = 0;
            if (j == TABLES_DEPTH)
            {
                PMrestore(prevAddress, virtualAddress >> OFFSET_WIDTH);
                return prevAddress*PAGE_SIZE + pageOff;
            }
            findFree(0, depth, &freeFrame, -1, 0, prevAddress, &maxFrame);
            PMwrite(prevAddress * PAGE_SIZE + pageOff, freeFrame);
            prevAddress = freeFrame; //useless in the last iteration
        }
        else{
            if (j == TABLES_DEPTH)
            {
                return prevAddress*PAGE_SIZE + pageOff;
            }
            prevAddress = currAddress; //useless in the last iteration

        }
    }
    return 1;
}


int VMread(uint64_t virtualAddress, word_t *value)
{
    uint64_t pAddress = getPhysicalAddress(virtualAddress, *value);
    PMread(pAddress, value);
    return 1;
}


int VMwrite(uint64_t virtualAddress, word_t value)
{
    uint64_t pAddress = getPhysicalAddress(virtualAddress, value);
    PMwrite(pAddress, value);
    return 1;
}


