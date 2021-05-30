#include <atomic>
#include <limits>
#include <pthread.h>
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"

typedef uint64_t UINT64;

#define MAX_UINT64 std::numeric_limits<UINT64>::max()
#define COUNTER_WIDTH 31
#define TOTAL_TASKS_WIDTH 31
#define STAGE_WIDTH 2


struct ThreadContext
{
    uint32_t threadID;
    JobContext* jobContext;
};

void updateMapPhase(std::atomic<UINT64> *stage)
{
    stage->exchange(*stage & (MAX_UINT64 & ((UINT64)MAP_STAGE<<COUNTER_WIDTH<<TOTAL_TASKS_WIDTH)));
}

class JobContext {
private:
    pthread_t *threads;
    const MapReduceClient& client;
    const InputVec& input;
    OutputVec& output;
    std::atomic<UINT64> stage;
    Barrier barrier;

    static void* runThread(void* thisObj)
    {
        JobContext *j = static_cast<ThreadContext*>(thisObj)->jobContext;
        updateMapPhase(&(j->stage));
        while(auto counter = j->stage++ < j->input.size())
        {
            auto item = j ->input[counter];
            j->client.map(item.first, item.second, j);
        }
        //todo everything :(
        return nullptr;
    }


public:
    JobContext(const MapReduceClient& client,
               const InputVec& inputVec, OutputVec& outputVec,
               int multiThreadLevel): client(client), input(inputVec), output(outputVec), stage(UNDEFINED_STAGE),
               barrier(multiThreadLevel)
    {
        threads = new pthread_t[multiThreadLevel]();
        for(uint32_t i = 0; i < multiThreadLevel; i++)
        {
            ThreadContext tc = {i, this};
            pthread_create(threads + i, nullptr, JobContext::runThread, static_cast<void*>(&tc));
        }
    }
    ~JobContext()
    {
        delete[] threads;
    }
};