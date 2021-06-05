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

stage_t getStage(uint64_t atomic){
    uint64_t mask = 3ULL << COUNTER_WIDTH << COUNTER_WIDTH;
    return static_cast<stage_t>((atomic & mask)>>62);

}
uint32_t getProgress(uint64_t atomic){
    uint64_t mask = (1ULL<<62)-1ULL-((1ULL<<31)-1ULL);
    return static_cast<uint32_t>((atomic & mask)>>31);
}

uint32_t getCounter(uint64_t atomic){
    uint64_t mask = (1ULL<<31)-1ULL;
    return static_cast<uint32_t> (atomic & mask);
}
void resetCounter(std::atomic<UINT64> *counter){
    *counter &= 3ULL << COUNTER_WIDTH << COUNTER_WIDTH;
}


void incrementStage(std::atomic<UINT64> *counter)
{
    *counter+= 1ULL << COUNTER_WIDTH << COUNTER_WIDTH;
    resetCounter(counter);
}

struct ThreadContext
{
    pthread_t thread;
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
    sem_t stageSem;
    int multiThreadLevel;
    std::vector<IntermediateVec> queue;
    bool isJoined;

    static void* runThread(void* thisObj)
    {
        auto *tc = static_cast<ThreadContext*>(thisObj);
        JobContext *j = tc->jobContext;
        if (tc -> threadID == 0)
        {
            incrementStage(&(j->counter));
        }
        else
        {
            sem_wait(&(j->stageSem));
        }
        sem_post(&(j->stageSem));
        auto size = j->input.size();
        auto i = getCounter(j->counter++);
        while(i < size) //atomically increase an index and get its previous value
        {
            auto item = j ->input[i];
            j->client.map(item.first, item.second, (void*)tc);
            j->counter += 1ULL << COUNTER_WIDTH;
            i = getCounter(j->counter++);
        }
//        unsigned int totalIntermediate = tc->intermediate.size();
        std::sort(tc->intermediate.begin(), tc->intermediate.end(), weak_order); //no race condition problem
        j->barrier.barrier();
        if (tc -> threadID != 0)
        {
            sem_wait(&j->shuffleSem); //stop all threads but the shuffling thread (zero)
        }
        else
        {
            IntermediateVec prev;
            incrementStage(&(j->counter));
            //todo shuffle
            for(int k = 0; k < tc->jobContext->multiThreadLevel; k++)
            {
                IntermediateVec dest;
                std::merge(prev.begin(),prev.end(), tc->jobContext->threads[k].intermediate.begin(),
                           tc->jobContext->threads[k].intermediate.end(), std::back_inserter(dest), weak_order);
                prev = dest;
            }
            j->totalWork = prev.size();
            IntermediateVec matchingPairs;
            IntermediatePair currPair = prev.front();
            for(IntermediatePair pair:prev){
                j -> counter++;
                if(!(*pair.first < *currPair.first) && !(*currPair.first < *pair.first)){
                    matchingPairs.push_back(pair);
                } else{
                    j -> queue.push_back(matchingPairs);
                    matchingPairs.clear();
                    currPair = pair;
                    matchingPairs.push_back(pair);
                }
                j->counter += 1ULL << COUNTER_WIDTH;
            }
            j->queue.push_back(matchingPairs);
            incrementStage(&(j->counter));
            j->totalWork = j->queue.size();
        }
        sem_post(&j->shuffleSem);
        auto k = getCounter(j->counter++);

//        int k = getCounter(j->counter++);
        while(k< j -> queue.size()) //atomically increase an index and get its previous value
        {
            const IntermediateVec* item = &j->queue[k];
            j->client.reduce(item, tc);
            k = getCounter(j->counter++);
            j->counter += 1ULL << COUNTER_WIDTH;
//            pthread_mutex_lock(&j->prettyPrint);
//            JobState js;
//            getJobState(j, &js);
//            std::cout << "percentage: " << js.percentage <<std::endl;
//            std::cout << "progress: " << getProgress(j->counter) <<std::endl;
//            std::cout << "total: " << j->totalWork <<std::endl;
//            pthread_mutex_unlock(&j->prettyPrint);
        }
//        if (getProgress(j->counter) >= j->totalWork)
//        {
//            incrementStage(&j->counter);
//            std::cout << "Stage: " << getStage(j->counter);
//        }
        return nullptr;
    }


public:
    OutputVec& output;
    std::atomic<uint64_t> counter;
    unsigned int totalWork;
    pthread_mutex_t prettyPrint; //todo debug
    JobContext(const MapReduceClient& client,
               const InputVec& inputVec, OutputVec& outputVec,
               int multiThreadLevel): client(client), input(inputVec), output(outputVec)
            ,barrier(multiThreadLevel), totalWork(input.size()), queue(), isJoined()
    {
        threads = new ThreadContext[multiThreadLevel];
        this->multiThreadLevel = multiThreadLevel;
        sem_init(&shuffleSem, 0, 0);
        sem_init(&stageSem, 0, 0);
        pthread_mutex_init(&prettyPrint, nullptr); //todo debug
        for(uint32_t i = 0; i < multiThreadLevel; i++)
        {
//            ThreadContext* currThreadContext = new ThreadContext;
//            currThreadContext->threadID = i;
//            currThreadContext->jobContext = this;
//            currThreadContext->intermediate = *new IntermediateVec;
            pthread_t thread = 0;
            threads[i] = {thread, IntermediateVec(), i, this};
            pthread_create(&thread, nullptr, &JobContext::runThread, (threads + i));
//            this->output = currThreadContext->jobContext->output;
        }
    }
    ~JobContext()
    {
        delete[] threads;
    }

    void joinContext()
    {
        if(isJoined)
        {
            return;
        }
        isJoined = true;
        for(int i=0; i < multiThreadLevel; i++)
        {
            auto pt = threads[i].thread;
            pthread_join(pt, nullptr);
        }
    }
};



JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    auto jc = new JobContext(client, inputVec, outputVec, multiThreadLevel);
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
}
void emit3 (K3* key, V3* value, void* context){

    auto *tc = static_cast<ThreadContext*>(context);
    pthread_mutex_lock(&tc -> jobContext->prettyPrint);
    tc->jobContext->output.push_back(OutputPair(key, value));
    pthread_mutex_unlock(&tc -> jobContext->prettyPrint);
}
void waitForJob(JobHandle job){
        auto jc = static_cast<JobContext*>(job);
        jc->joinContext();
}

void getJobState(JobHandle job, JobState* state){
    auto jc = static_cast<JobContext*>(job);
    auto currState = getStage(jc->counter);
    state->stage = static_cast<stage_t>(currState);
    state->percentage = ((float)getProgress(jc->counter) / (float)(jc->totalWork)) * 100; //todo precision after decimal
}
void closeJobHandle(JobHandle job) {
    waitForJob(job);
    auto jc = static_cast<JobContext*>(job);
    delete jc;
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