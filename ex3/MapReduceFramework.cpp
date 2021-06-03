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

int getStage(uint64_t atomic){
    uint64_t mask = -1ULL-((1ULL<<62)-1ULL);
    return (int) ((atomic & mask)>>62);

}
int getAll(uint64_t atomic){
    uint64_t mask = (1ULL<<62)-1ULL-((1ULL<<31)-1ULL);
    return (int) ((atomic & mask)>>31);
}

int getCounter(uint64_t atomic){
    uint64_t mask = (1ULL<<31)-1ULL;
    return (int) atomic & mask;
}
int resetCounter(uint64_t atomic){
//    todo
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
    Barrier barrier;
    sem_t shuffleSem;
    int multiThreadLevel;

    static void* runThread(void* thisObj)
    {
        auto *tc = static_cast<ThreadContext*>(thisObj);
        JobContext *j = tc->jobContext;
//        updatePhase(&(j->stage), MAP_STAGE);
        unsigned long size = j->input.size();
        //todo which counter to update
        j->counter+=(size<<31);
        int i = getCounter(j->counter++);
        while(i < size) //atomically increase an index and get its previous value
        {
            auto item = j ->input[i];
            j->client.map(item.first, item.second, (void*)tc);
            i = getCounter(j->counter++);
        }
        unsigned int totalIntermediate = tc->intermediate.size();
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
//            updatePhase(&(j->stage), SHUFFLE_STAGE);
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
        //updatePhase(&(j->stage), REDUCE_STAGE);
        //todo fix reset - only reset first part
        j->counter = 0;
        sem_destroy(&j->shuffleSem);
        //todo which counter to update
        int k = getCounter(j->counter++);
        while(k< queue.size()) //atomically increase an index and get its previous value
        {
            std::cout<<k<<std::endl;
            const IntermediateVec* item = &queue[k];
            j->client.reduce(item, tc);
            k = getCounter(j->counter++);
        }
        return 0;
    }


public:
    OutputVec& output;
    std::atomic<uint64_t> counter;
    JobContext(const MapReduceClient& client,
               const InputVec& inputVec, OutputVec& outputVec,
               int multiThreadLevel): client(client), input(inputVec), output(outputVec)
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
            pthread_create(&threadId, nullptr, &JobContext::runThread, (threads + i));
            this->output = currThreadContext->jobContext->output;
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
    return jc;
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
    //todo which counter to update
    tc->jobContext->counter++;
}
void emit3 (K3* key, V3* value, void* context){

    auto *tc = static_cast<ThreadContext*>(context);
    tc->jobContext->output.push_back(OutputPair(key, value));
    //todo which counter to update
    tc->jobContext->counter++;
}
void waitForJob(JobHandle job){
//    todo

}
void getJobState(JobHandle job, JobState* state){
    JobContext* jc = static_cast<JobContext*>(job);
    auto currState = getStage(jc->counter);
    state->stage = static_cast<stage_t>(currState);
//    todo change
    state->percentage = 10.0;
}
void closeJobHandle(JobHandle job){

}


// todo avoid std::merge
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