#include <atomic>
#include <limits>
#include <algorithm>
#include <pthread.h>
#include <semaphore.h>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"

typedef uint64_t UINT64;

#define MAX_UINT64 std::numeric_limits<UINT64>::max()
#define COUNTER_WIDTH 31
#define TOTAL_TASKS_WIDTH 31
#define STAGE_WIDTH 2
class JobContext;
bool weak_order(const IntermediatePair &pair1, const IntermediatePair &pair2)
{
    return pair1.first < pair2.first;
}





/**
 * Update the phase of the map reduce between different stage_t values
 * @param stage The stage of the mapreduce. First STAGE_WIDTH bits are dedicated to the stage
 * @param st the stage to update
 */
void updatePhase(std::atomic<UINT64> *stage, stage_t st)
{
    stage->exchange(*stage & (MAX_UINT64 & ((UINT64)st<<COUNTER_WIDTH<<TOTAL_TASKS_WIDTH)));
}
struct ThreadContext
{
    pthread_t *thread;
    IntermediateVec intermediate; //each thread's intermediate vector
    uint32_t threadID;
    JobContext* jobContext; //holds all other refrences
    int multiThreadLevel;
};
class JobContext {
private:
    ThreadContext *threads;
    const MapReduceClient& client;
    const InputVec& input;
    OutputVec& output; //todo use
    // 2 MSB is stage_t (map, shuffle, undefined...) 31 LSB are the counter
    std::atomic<UINT64> stage;
    Barrier barrier;
    sem_t shuffleSem;

    static void* runThread(void* thisObj)
    {
        auto *tc = static_cast<ThreadContext*>(thisObj);
        JobContext *j = tc->jobContext;
        updatePhase(&(j->stage), MAP_STAGE);
        while(auto counter = j->stage++ < j->input.size()) //atomically increase an index and get its previous value
        {
            auto item = j ->input[counter];
            j->client.map(item.first, item.second, (void*)tc);
        }
        std::sort(tc->intermediate.begin(), tc->intermediate.end(), weak_order); //no race condition problem
        j->barrier.barrier();
        unsigned long numPairs = tc->jobContext->threads->intermediate.size();
        IntermediateVec* intermediateVectors = new IntermediateVec[numPairs];
        if (tc -> threadID != 0)
        {
            std::cout<<"waiting: "<<tc->threadID<<std::endl;
            sem_wait(&j->shuffleSem); //stop all threads but the shuffling thread (zero)
        }
        else
        {

            std::cout<<"shuffling: "<<tc->threadID<<std::endl;
            updatePhase(&(j->stage), SHUFFLE_STAGE);
            //todo shuffle
            int isEmpty = 0;
            while(isEmpty <= tc->multiThreadLevel)
            {
                IntermediateVec newAssignment;
                for(int k = 0; k < tc->multiThreadLevel; k++)
                {
                    if (tc->jobContext->threads[k].intermediate.empty()){
                        isEmpty +=1;
                    }
                    else
                    {
                        IntermediatePair topPair = tc->jobContext->threads[k].intermediate.back();
                        newAssignment.push_back(topPair);
                        tc->jobContext->threads[k].intermediate.pop_back();
                    }
                }
                intermediateVectors[numPairs-tc->jobContext->threads->intermediate.size()] = newAssignment;
            }
        }
        sem_post(&j->shuffleSem);
        //todo reduce
        return nullptr;
    }


public:
    JobContext(const MapReduceClient& client,
               const InputVec& inputVec, OutputVec& outputVec,
               int multiThreadLevel): client(client), input(inputVec), output(outputVec), stage(UNDEFINED_STAGE)
               ,barrier(multiThreadLevel)
    {
        threads = new ThreadContext[multiThreadLevel];
        threads->multiThreadLevel = multiThreadLevel;
        sem_init(&shuffleSem, 0, 0);
        for(uint32_t i = 0; i < multiThreadLevel; i++)
        {
            threads[i].threadID = i;
            threads[i].jobContext = this;
            pthread_t threadId;
//            pthread_create(threads[i].thread, nullptr, &JobContext::runThread, static_cast<void*>(threads + i));
            pthread_create(&threadId, nullptr, &JobContext::runThread, static_cast<void*>(threads + i));
        }
    }
    ~JobContext()
    {
        delete[] threads;
    }
};



JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    JobContext* jc = new JobContext(client, inputVec, outputVec, multiThreadLevel);

}

/**
 * The function receives as input intermediary element (K2, V2) and context which contains
    data structure of the thread that created the intermediary element. The function saves the
    intermediary element in the context data structures. In addition, the function updates the
    number of intermediary elements using atomic counter.
 * @param key
 * @param value
 * @param context
 */
void emit2 (K2* key, V2* value, void* context){

    auto *tc = static_cast<ThreadContext*>(context);
    tc->intermediate.push_back(IntermediatePair(key, value));
    //todo increment intermid counter
    return;
}
void emit3 (K3* key, V3* value, void* context){
    return;
}
void waitForJob(JobHandle job){

}
void getJobState(JobHandle job, JobState* state){

}
void closeJobHandle(JobHandle job){

}

