#include "VirtualMemory.h"
#include "PhysicalMemory.h"
#define FAILURE 0
#define SUCCESS 1


uint64_t getPhysicalAddress(uint64_t virtualAddress);

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

void getPhysicalValue(uint64_t frame, uint64_t offset, word_t *value)
{
    PMread(frame * PAGE_SIZE + offset, value);
}


static void translateAddr(uint64_t virtualAddress, uint64_t *buffer)
{
    for (int i = TABLES_DEPTH; i >=0; i--)
    {
        buffer[i] = virtualAddress & ((1ULL << OFFSET_WIDTH) - 1);
        virtualAddress = virtualAddress >> OFFSET_WIDTH;
    }
}


static void
evict(uint64_t address, int *evictedFrame, uint64_t *pageVAddr,
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

                evict(value, evictedFrame, pageVAddr, evictParent,
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
            *evictedFrame = address;
            *evictParent = parentAddr;
            *pageVAddr = accumOff;
        }
    }


    if (depth == 0)
    {
        PMevict(*evictedFrame, *pageVAddr);
        clearTable(*evictedFrame);
        PMwrite(*evictParent * PAGE_SIZE + (((1ULL << OFFSET_WIDTH) - 1) & *pageVAddr), 0);
    }
}


static void findFree(uint64_t frameAddr, int depth, int *freeFrame, uint64_t parentNode, uint64_t childOffset,
                     uint64_t selfNode, uint64_t *maxFrame)
{
    *maxFrame = frameAddr > *maxFrame ? frameAddr : *maxFrame;
    if (depth == TABLES_DEPTH)
    {
        return;
    }
    int freeCells = 0;
    word_t value = 0;
    for (int off = 0; off < PAGE_SIZE; ++off)
    {
        getPhysicalValue(frameAddr, off, &value);
        if (value == 0)
        {
            freeCells++;
        }
        else
        {
            if (*freeFrame == -1) //todo freeFrame type
            {
                findFree(value, depth + 1, freeFrame, frameAddr, off, selfNode, maxFrame);
            }
            else
            {
                return;
            }
        }
    }
    if (freeCells == PAGE_SIZE && frameAddr != selfNode && depth < TABLES_DEPTH)
    { //case 1=
        PMwrite(parentNode*PAGE_SIZE + childOffset, 0); // reset node
        *freeFrame = frameAddr;
    }  else if (depth == 0 && *freeFrame == -1) //2, 3
    { //finished recursion w/o finding a freeFrame frame

        if (*maxFrame < NUM_FRAMES - 1)
        { //case 2
            *freeFrame = *maxFrame + 1;
            clearTable(*freeFrame);
        } else
        {
            uint64_t evictOffset = 0;
            uint64_t evictParent = 0;
            int maxWeight = 0;
            evict(0, freeFrame, &evictOffset, &evictParent, 0, 0, 0, 0, &maxWeight); //case 3
        }
    }
}




uint64_t getPhysicalAddress(uint64_t virtualAddress)
{
    uint64_t pagesOffsets[TABLES_DEPTH+1];
    translateAddr(virtualAddress, pagesOffsets);
    word_t currAddress = 0;
    uint64_t prevAddress = 0;
    uint64_t pageOff = 0;
    for (int depth = 0; depth < TABLES_DEPTH; depth++)
    {
        pageOff = pagesOffsets[depth];
        getPhysicalValue(prevAddress, pageOff, &currAddress);
        if (currAddress == 0)
        {
            int freeFrame = -1;
            uint64_t maxFrame = 0;
            findFree(0, 0, &freeFrame, -1, 0, prevAddress, &maxFrame);
            PMwrite(prevAddress * PAGE_SIZE + pageOff, freeFrame);
            prevAddress = freeFrame; //useless in the last iteration
            if (depth == TABLES_DEPTH - 1)
            {
                PMrestore(freeFrame, virtualAddress >> OFFSET_WIDTH);
            }
        }
        else
        {
            prevAddress = currAddress; //useless in the last iteration

        }
    }
    return prevAddress * PAGE_SIZE + pagesOffsets[TABLES_DEPTH];
}


int VMread(uint64_t virtualAddress, word_t *value)
{
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE)
    {
        return FAILURE;
    }
    uint64_t pAddress = getPhysicalAddress(virtualAddress);
    PMread(pAddress, value);
    return SUCCESS;
}


int VMwrite(uint64_t virtualAddress, word_t value)
{
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE)
    {
        return FAILURE;
    }
    uint64_t pAddress = getPhysicalAddress(virtualAddress);
    PMwrite(pAddress, value);
    return SUCCESS;
}


