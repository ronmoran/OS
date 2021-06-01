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
    return *pair1.first < *pair2.first;
}


bool order_vec(const IntermediateVec &v1, const IntermediateVec &v2)
{
    return weak_order(v1.back(), v2.back());
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

};
class JobContext {
private:
    ThreadContext *threads;
    const MapReduceClient& client;
    const InputVec& input;
//    OutputVec& output; //todo use
    // 2 MSB is stage_t (map, shuffle, undefined...) 31 LSB are the counter
    std::atomic<uint64_t> stage;
    std::atomic<uint64_t> counter;
    std::atomic<uint64_t> counterReduce;
    Barrier barrier;
    sem_t shuffleSem;
    int multiThreadLevel;

    static void* runThread(void* thisObj)
    {
        auto *tc = static_cast<ThreadContext*>(thisObj);
        JobContext *j = tc->jobContext;
        updatePhase(&(j->stage), MAP_STAGE);
        int i = j->counter++;
        while(i< j->input.size()) //atomically increase an index and get its previous value
        {
            auto item = j ->input[i];
            j->client.map(item.first, item.second, (void*)tc);
            i = j->counter++;
        }
        std::sort(tc->intermediate.begin(), tc->intermediate.end(), weak_order); //no race condition problem
        j->barrier.barrier();
        std::vector<IntermediateVec> queue;
        IntermediateVec prev;

        if (tc -> threadID != 0)
        {
            sem_wait(&j->shuffleSem); //stop all threads but the shuffling thread (zero)
        }
        else
        {
            updatePhase(&(j->stage), SHUFFLE_STAGE);
            //todo shuffle
            for(int k = 0; k < tc->jobContext->multiThreadLevel; k++)
            {
                IntermediateVec dest;
                std::merge(prev.begin(),prev.end(), tc->jobContext->threads[k].intermediate.begin(),
                           tc->jobContext->threads[k].intermediate.end(), std::back_inserter(dest), weak_order);
                prev = dest;

            }
            IntermediateVec matchingPairs;
            IntermediatePair currPair = prev.front();
            for(IntermediatePair pair:prev){
                if(!(*pair.first < *currPair.first) && !(*currPair.first < *pair.first)){
                    matchingPairs.push_back(pair);
                } else{
                    queue.push_back(matchingPairs);
                    matchingPairs.clear();
                    currPair = pair;
                    matchingPairs.push_back(pair);
                }
            }
            queue.push_back(matchingPairs);
        }
        sem_post(&j->shuffleSem);

        updatePhase(&(j->stage), REDUCE_STAGE);
        int k = j->counterReduce++;
        while(k< queue.size()) //atomically increase an index and get its previous value
        {
            const IntermediateVec* item = &queue[i];
            j->client.reduce(item, tc);
            k = j->counterReduce++;
        }
        //todo reduce
        return nullptr;
    }


public:
    OutputVec& output; //todo use
    JobContext(const MapReduceClient& client,
               const InputVec& inputVec, OutputVec& outputVec,
               int multiThreadLevel): client(client), input(inputVec), output(outputVec), stage(UNDEFINED_STAGE)
               ,barrier(multiThreadLevel)
    {
        threads = new ThreadContext[multiThreadLevel];
        this->multiThreadLevel = multiThreadLevel;
        sem_init(&shuffleSem, 0, 0);
        for(uint32_t i = 0; i < multiThreadLevel; i++)
        {
            ThreadContext* currThreadContext = new ThreadContext;
            currThreadContext->threadID = i;
            currThreadContext->jobContext = this;
            currThreadContext->intermediate = *new IntermediateVec;
            pthread_t threadId;
            threads[i] = *currThreadContext;
//            pthread_create(threads[i].thread, nullptr, &JobContext::runThread, static_cast<void*>(threads + i));
            pthread_create(&threadId, nullptr, &JobContext::runThread, (threads + i));
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
    auto *tc = static_cast<ThreadContext*>(context);
    tc->jobContext->output.push_back(OutputPair(key, value));
    //todo increment counter
    return;
}
void waitForJob(JobHandle job){

}
void getJobState(JobHandle job, JobState* state){

}
void closeJobHandle(JobHandle job){

}

//int isEmpty = 0;
//            while(isEmpty <= tc->multiThreadLevel)
//            {
//                IntermediatePair currPair;
//                IntermediateVec newAssignment;
//                for(int k = 0; k < tc->multiThreadLevel; k++)
//                {
//                    if (tc->jobContext->threads[k].intermediate.empty()){
//                        isEmpty +=1;
//                    }
//                    else
//                    {
//                        IntermediatePair topPair = tc->jobContext->threads[k].intermediate.back();
//                        newAssignment.push_back(topPair);
//                        tc->jobContext->threads[k].intermediate.pop_back();
//                    }
//                }
//                queue.push_back(newAssignment);
//            }