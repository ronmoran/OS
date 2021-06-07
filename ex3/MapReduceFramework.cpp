#include <atomic>
#include <algorithm>
#include <pthread.h>
#include <semaphore.h>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"

typedef std::atomic<uint64_t> atomic64;

#define COUNTER_WIDTH 31
class JobContext;

/**
 * Apply weak "<" order between IntermdiatePair objects for comparison.
 * They are compared based on the "first" attribute
 * @param pair1
 * @param pair2
 * @return  if pair 1 is less than pair two
 */
bool weak_order(const IntermediatePair &pair1, const IntermediatePair &pair2)
{
    return *pair1.first < *pair2.first;
}

/**
 * Get the stage property of the job context - 2 MSB to be exact
 * @param atomic The atomic counter of the map-reduce job
 * @return the current stage
 */
stage_t getStage(uint64_t atomic){
    uint64_t mask = 3ULL << COUNTER_WIDTH << COUNTER_WIDTH;
    return static_cast<stage_t>((atomic & mask)>>62);
}

/**
 * Get the number of finished jobs - 31 MSB after the 2 MSB
 * @param atomic
 * @return num of finished jobs
 */
uint32_t getProgress(uint64_t atomic){
    uint64_t mask = (1ULL<<62)-1ULL-((1ULL<<31)-1ULL); // mask of 31 bits of ones: 00111...111000...000
    return static_cast<uint32_t>((atomic & mask)>>31);
}

/**
 * Get the number of started jobs - 31 LSB
 * @param atomic
 * @return num of started jobs
 */
uint32_t getCounter(uint64_t atomic){
    uint64_t mask = (1ULL<<31)-1ULL; // mask of 31 1-bits 000...000111...111
    return static_cast<uint32_t> (atomic & mask);
}

/**
 * Reset the finished jobs and started jobs counter
 * @param counter
 */
void resetCounter(atomic64 *counter){
    *counter &= 3ULL << COUNTER_WIDTH << COUNTER_WIDTH;
}


/**
 * Increment to the next stage and reset the count of jobs
 * @param counter
 */
void setStage(atomic64 *counter, stage_t stage)
{
    *counter = stage * 1ULL << COUNTER_WIDTH << COUNTER_WIDTH;
//    resetCounter(counter);
}

/**
 * Basic array to hold the thread's attributes
 */
struct ThreadContext
{
    pthread_t thread;
    IntermediateVec intermediate; //each thread's intermediate vector
    uint32_t threadID;
    JobContext* jobContext; //holds all other references

};


/**
 * The MapReduce representation. Holds all the worker threads and sync primitives.
 */
class JobContext {
private:
    ThreadContext *threads;
    const MapReduceClient& client;
    const InputVec& input;
    Barrier barrier;
    sem_t shuffleSem;
    sem_t stageSem;
    uint32_t multiThreadLevel;
    std::vector<IntermediateVec> queue;
    pthread_mutex_t joinMutex;
    bool isJoined;

    // The main thread function - map, shuffle reduce and in between
    static void* runThread(void* threadContext)
    {
        auto *tc = static_cast<ThreadContext*>(threadContext);
        JobContext *j = tc->jobContext;
        map(*tc);
        if (tc -> threadID != 0)
        {
            sem_wait(&j->shuffleSem); //stop all threads but the shuffling thread (zero)
        }
        else
        {
            shuffle(*tc);
        }
        sem_post(&j->shuffleSem); //resume threads, one after another
        reduce(*tc);
        return nullptr;
    }

    void static map(ThreadContext& tc)
    {
        JobContext *j = tc.jobContext;
        if (tc.threadID != 0)
        {
            sem_wait((&j->stageSem));
        }
        else
        {
            setStage(&j->counter, MAP_STAGE);
        }
        sem_post(&j->stageSem);
        auto size = j->input.size();
        auto i = getCounter(j->counter++);
        while(i < size) //atomically increase an index and get its previous value
        {
            auto item = j ->input[i];
            j->client.map(item.first, item.second, (void*)&tc);
            j->counter += 1ULL << COUNTER_WIDTH;
            i = getCounter(j->counter++);
        }
        std::sort(tc.intermediate.begin(), tc.intermediate.end(), weak_order); //no race condition problem
        j->barrier.barrier();
    }

    void static shuffle(ThreadContext& tc)
    {
        JobContext *j = tc.jobContext;
        IntermediateVec prev;
        setStage(&(j->counter), SHUFFLE_STAGE);
        unsigned int q_size = 0;
        for(uint32_t k = 0; k < j->multiThreadLevel; k++)
        {
            q_size += j->threads[k].intermediate.size();
        }
        for(uint32_t k = 0; k < j->multiThreadLevel; k++)
        {
            IntermediateVec dest;
            std::merge(prev.begin(),prev.end(), j->threads[k].intermediate.begin(),
                       j->threads[k].intermediate.end(), std::back_inserter(dest), weak_order);
            prev = dest;
        }
        j->totalWork = prev.size();
        IntermediateVec matchingPairs;
        IntermediatePair currPair = prev.front();
        for(IntermediatePair pair:prev){
            j -> counter++;
            if(!(*pair.first < *currPair.first) && !(*currPair.first < *pair.first)){
                matchingPairs.push_back(pair); //single key
            } else{
                j -> queue.push_back(matchingPairs); //add single key
                matchingPairs.clear();
                currPair = pair;
                matchingPairs.push_back(pair);
            }
            j->counter += 1ULL << COUNTER_WIDTH;
        }
        j->queue.push_back(matchingPairs);
        setStage(&(j->counter), REDUCE_STAGE);
        j->totalWork = j->queue.size();
    }

    static void reduce(ThreadContext &tc)
    {
        auto *j = static_cast<JobContext*>(tc.jobContext);
        auto k = getCounter(j->counter++);
        while(k< j -> queue.size()) //atomically increase an index and get its previous value
        {
            const IntermediateVec* item = &j->queue[k];
            j->client.reduce(item, &tc);
            k = getCounter(j->counter++);
            j->counter += 1ULL << COUNTER_WIDTH;
        }
    }


public:
    OutputVec& output;
    std::atomic<uint64_t> counter;
    unsigned int totalWork;
    pthread_mutex_t emitMutex;
    JobContext(const MapReduceClient& client,
               const InputVec &inputVec, OutputVec &outputVec,
               int multiThreadLevel) : client(client), input(inputVec), barrier(multiThreadLevel),
                                       shuffleSem(), stageSem(), multiThreadLevel(multiThreadLevel),
                                       queue(), isJoined(false),
                                       output(outputVec), counter(), totalWork(input.size()), emitMutex()
    {
        threads = new ThreadContext[multiThreadLevel]();
        sem_init(&shuffleSem, 0, 0);
        sem_init(&stageSem, 0, 0);
        pthread_mutex_init(&emitMutex, nullptr);
        pthread_mutex_init(&joinMutex, nullptr);
        for(uint32_t i = 0; i < (uint32_t)multiThreadLevel; i++)
        {
            pthread_t thread = 0;
            threads[i] = {thread, IntermediateVec(), i, this};
            pthread_create(&threads[i].thread, nullptr, &JobContext::runThread, (threads + i));
        }
    }
    ~JobContext()
    {
        delete[] threads;
        pthread_mutex_destroy(&emitMutex);
        pthread_mutex_destroy(&joinMutex);
        sem_destroy(&shuffleSem);
        sem_destroy(&stageSem);
    }

    void joinContext()
    {
        pthread_mutex_lock(&joinMutex);
        if (!isJoined)
        {
            for (uint32_t i = 0; i < multiThreadLevel; i++)
            {
                auto pt = threads[i].thread;
                pthread_join(pt, nullptr);
            }
        isJoined = true;
        }
        pthread_mutex_unlock(&joinMutex);
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
 * @param context has to be castable to a sensible JobContext object
 */
void emit2 (K2* key, V2* value, void* context){

    auto *tc = static_cast<ThreadContext*>(context);
    tc->intermediate.push_back(IntermediatePair(key, value));
}
void emit3 (K3* key, V3* value, void* context){

    auto *tc = static_cast<ThreadContext*>(context);
    pthread_mutex_lock(&tc -> jobContext->emitMutex);
    tc->jobContext->output.push_back(OutputPair(key, value));
    pthread_mutex_unlock(&tc -> jobContext->emitMutex);
}
void waitForJob(JobHandle job){
        auto jc = static_cast<JobContext*>(job);
        jc->joinContext();
}

void getJobState(JobHandle job, JobState* state){
    auto jc = static_cast<JobContext*>(job);
    uint64_t counter = jc->counter.load();
    state->percentage = ((float)getProgress(counter) / (float)(jc->totalWork)) * 100; //todo precision
    state->stage = static_cast<stage_t>(getStage(counter));
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