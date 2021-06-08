#include <atomic>
#include <algorithm>
#include <pthread.h>
#include <semaphore.h>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"

#define COUNTER_WIDTH 31
#define SHUFFLE_THREAD_ID 0


typedef std::atomic<uint64_t> atomic64;
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
uint32_t getNumFinished(uint64_t atomic){
    uint64_t mask = (1ULL<<62)-1ULL-((1ULL<<31)-1ULL); // mask of 31 bits of ones: 00111...111000...000
    return static_cast<uint32_t>((atomic & mask)>>31);
}

/**
 * Get the number of started jobs - 31 LSB
 * @param counter
 * @return num of started jobs
 */
uint32_t getNumStarted(uint64_t counter){
    uint64_t mask = (1ULL<<31)-1ULL; // mask of 31 1-bits 000...000111...111
    return static_cast<uint32_t> (counter & mask);
}


/**
 * Increment to the next stage and reset the count of jobs (started and finished)
 * @param counter
 */
void setStage(atomic64 *counter, stage_t stage)
{
    *counter = stage * 1ULL << COUNTER_WIDTH << COUNTER_WIDTH;
//    resetCounter(counter);
}

/**
 * Basic class to hold the thread's attributes
 */
class ThreadContext
{
private:
    pthread_t thread;
    IntermediateVec intermediate; //each thread's intermediate vector
    uint32_t threadID;
    JobContext* jobContext; //holds all other references
public:

    ThreadContext(): thread(), intermediate(), threadID(), jobContext() {}

    /**
     * Create a new thread context object from parameters.
     * @param thread
     * @param intermediate Note that the values are moved and therefore might null values in this argument
     * @param threadId
     * @param jobContext
     */
    ThreadContext(pthread_t thread, IntermediateVec intermediate, uint32_t threadId, JobContext *jobContext)
            : thread(thread), intermediate(intermediate), threadID(threadId), jobContext(jobContext)
    {
        this -> intermediate = std::move(intermediate);
    }

    ThreadContext& operator=(const ThreadContext &tc)
    {
        if (&tc != this)
        {
            thread = tc.thread;
            intermediate = tc.intermediate;
            threadID = tc.threadID;
            jobContext = tc.jobContext;
        }
        return *this;
    }

    pthread_t& getThread()
    {
        return thread;
    }

    IntermediateVec &getIntermediate()
    {
        return intermediate;
    }

    uint32_t getThreadId() const
    {
        return threadID;
    }

    JobContext *getJobContext() const
    {
        return jobContext;
    }

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
    sem_t stageSetSem;
    uint32_t multiThreadLevel;
    std::vector<IntermediateVec> queue;
    pthread_mutex_t joinMutex;
    bool isJoined;
    OutputVec& output;

    // The main thread function - map, shuffle reduce and in between
    static void* runThread(void* threadContext);

    void static map(ThreadContext *tc);

    void shuffle();

    static void reduce(ThreadContext *tc);


public:
    std::atomic<uint64_t> counter;
    size_t totalWork;
    pthread_mutex_t emitMutex;
    JobContext(const MapReduceClient& client,
               const InputVec &inputVec, OutputVec &outputVec,
               int multiThreadLevel);

    ~JobContext();

    void joinContext()
    {
        pthread_mutex_lock(&joinMutex);
        if (!isJoined)
        {
            for (uint32_t i = 0; i < multiThreadLevel; i++)
            {
                pthread_join(threads[i].getThread(), nullptr);
            }
        isJoined = true;
        }
        pthread_mutex_unlock(&joinMutex);
    }

    void getJobState(JobState* state);

    void writeToOutput(K3 *key, V3 *value);

    void sortIntermediates(IntermediateVec &tmpIntermediate) const;

    void joinSortedKeys(std::vector<IntermediatePair> &tmp);
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
    tc->getIntermediate().push_back(IntermediatePair(key, value));
}
void emit3 (K3* key, V3* value, void* context){

    auto *tc = static_cast<ThreadContext*>(context);
    tc -> getJobContext() ->writeToOutput(key, value);
}
void waitForJob(JobHandle job){
    auto jc = static_cast<JobContext*>(job);
    jc->joinContext();
}

void getJobState(JobHandle job, JobState* state){
    auto jc = static_cast<JobContext*>(job);
    jc -> getJobState(state);
}

void closeJobHandle(JobHandle job) {
    waitForJob(job);
    auto jc = static_cast<JobContext*>(job);
    delete jc;
}

JobContext::JobContext(const MapReduceClient &client,
                       const InputVec &inputVec, OutputVec &outputVec,
                       int multiThreadLevel) : client(client), input(inputVec), barrier(multiThreadLevel),
                                               shuffleSem(), stageSetSem(),
                                               multiThreadLevel((uint32_t) multiThreadLevel),
                                               queue(), joinMutex(), isJoined(false),
                                               output(outputVec), counter(), totalWork(input.size()), emitMutex()
{
    threads = new ThreadContext[multiThreadLevel]();
    sem_init(&shuffleSem, 0, 0);
    sem_init(&stageSetSem, 0, 0);
    pthread_mutex_init(&emitMutex, nullptr);
    pthread_mutex_init(&joinMutex, nullptr);
    for (uint32_t i = 0; i < this->multiThreadLevel; i++)
    {
        pthread_t thread = 0;
        threads[i] = ThreadContext(thread, IntermediateVec(), i, this);
        pthread_create(&threads[i].getThread(), nullptr, &JobContext::runThread, (threads + i));
    }
}

JobContext::~JobContext()
{
    delete[] threads;
    pthread_mutex_destroy(&emitMutex);
    pthread_mutex_destroy(&joinMutex);
    sem_destroy(&shuffleSem);
    sem_destroy(&stageSetSem);
}

void JobContext::getJobState(JobState *state)
{
    uint64_t counter_copy = counter.load();
    state->percentage = ((float) getNumFinished(counter_copy) / (float) (totalWork)) * 100; //todo precision
    state->stage = static_cast<stage_t>(getStage(counter_copy));
}

void JobContext::writeToOutput(K3* key, V3* value)
{
    pthread_mutex_lock(&emitMutex);
    output.push_back(OutputPair(key, value));
    pthread_mutex_unlock(&emitMutex);
}


/**  private **/

void JobContext::shuffle()
{
    setStage(&counter, SHUFFLE_STAGE);
    auto tmp = IntermediateVec();
    sortIntermediates(tmp);
    totalWork = tmp.size();
    joinSortedKeys(tmp);
    setStage(&(counter), REDUCE_STAGE);
    totalWork = queue.size();
}

void JobContext::joinSortedKeys(std::vector<IntermediatePair> &tmp)
{
    IntermediateVec matchingPairs;
    IntermediatePair currPair = tmp.front();
    for(IntermediatePair pair:tmp){
        counter++;
        if(!(*pair.first < *currPair.first) && !(*currPair.first < *pair.first)){
            matchingPairs.push_back(pair); //single key
        } else{
            queue.push_back(matchingPairs); //add single key
            matchingPairs.clear();
            currPair = pair;
            matchingPairs.push_back(pair);
        }
        counter += 1ULL << COUNTER_WIDTH;
    }
    queue.push_back(matchingPairs);
}

void JobContext::sortIntermediates(IntermediateVec &tmpIntermediate) const
{
    unsigned int q_size = 0;
    for(uint32_t k = 0; k < multiThreadLevel; k++)
    {
        q_size += threads[k].getIntermediate().size();
    }
    tmpIntermediate.resize(q_size);
    size_t ind = 0;
    for(uint32_t k = 0; k < multiThreadLevel; k++)
    {
        std::copy(threads[k].getIntermediate().cbegin(),
                  threads[k].getIntermediate().cend(),
                  tmpIntermediate.begin() + (std::iterator_traits<IntermediateVec::iterator>::difference_type) ind);
        ind += threads[k].getIntermediate().size();
    }
    std::sort(tmpIntermediate.begin(), tmpIntermediate.end(), weak_order);
}

void *JobContext::runThread(void *threadContext)
{
    auto *tc = static_cast<ThreadContext *>(threadContext);
    JobContext *j = tc->getJobContext();
    map(tc);
    if (tc->getThreadId() != SHUFFLE_THREAD_ID)
    {
        sem_wait(&j->shuffleSem); //stop all threads but the shuffling thread (zero)
    } else
    {
        j->shuffle();
    }
    sem_post(&j->shuffleSem); //resume threads, one after another
    reduce(tc);
    return nullptr;
}


void JobContext::map(ThreadContext *tc)
{
    JobContext *j = tc->getJobContext();
    if (tc->getThreadId() != SHUFFLE_THREAD_ID)
    {
        sem_wait((&j->stageSetSem));
    } else
    {
        setStage(&j->counter, MAP_STAGE);
    }
    sem_post(&j->stageSetSem);
    auto size = j->input.size();
    auto i = getNumStarted(j->counter++);
    while (i < size) //atomically increase an index and get its previous value
    {
        auto item = j->input[i];
        j->client.map(item.first, item.second, (void *) tc);
        j->counter += 1ULL << COUNTER_WIDTH;
        i = getNumStarted(j->counter++);
    }
    j->barrier.barrier();
}

void JobContext::reduce(ThreadContext *tc)
{
    auto *j = (tc -> getJobContext());
    auto k = getNumStarted(j->counter++);
    while(k< j -> queue.size()) //atomically increase an index and get its previous value
    {
        const IntermediateVec* item = &j->queue[k];
        j->client.reduce(item, tc);
        k = getNumStarted(j->counter++);
        j->counter += 1ULL << COUNTER_WIDTH;
    }
}
