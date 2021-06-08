#include <atomic>
#include <algorithm>
#include <pthread.h>
#include <semaphore.h>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"

#define COUNTER_WIDTH 31
#define SHUFFLE_THREAD_ID 0
#define SUCCESS 0
#define ERROR_EXIT 1
#define PERCENT_FACTOR 100
#define GENERAL_ERROR "system error: "
#define MUTEX_LOCK_ERROR "Unable to acquire mutex"
#define MUTEX_UNLOCK_ERROR "Unable to release mutex"
#define SEMAPHORE_WAIT_ERROR "Unable to wait for semaphore"
#define SEMAPHORE_POST_ERROR "Unable to post to semaphore"

typedef std::atomic<uint64_t> atomic64;

class JobContext;

class ThreadContext;

/** mask of 2 bits at [62-63] */
uint64_t MSB_BITS_MASK = 3ULL << COUNTER_WIDTH << COUNTER_WIDTH;

/** mask of 31 bits  at [31-61] */
uint64_t MID_BITS_MASK = (1ULL << COUNTER_WIDTH << COUNTER_WIDTH) - 1ULL - ((1ULL << COUNTER_WIDTH) - 1ULL);

/** mask of 31 bits at [0-30] */
uint64_t LSB_BITS_MASK = (1ULL << COUNTER_WIDTH) - 1ULL; // mask of 31 1-bits 000...000111...111

void error_exit(const std::string &message)
{
    std::cerr << GENERAL_ERROR << message << std::endl;
    exit(ERROR_EXIT);
}

/**
 * Apply weak "<" order between IntermediatePair objects for comparison.
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
stage_t getStage(uint64_t atomic)
{
    return static_cast<stage_t>((atomic & MSB_BITS_MASK) >> COUNTER_WIDTH >> COUNTER_WIDTH);
}

/**
 * Get the number of finished jobs - 31 MSB after the 2 MSB
 * @param atomic
 * @return num of finished jobs
 */
uint32_t getNumFinished(uint64_t atomic)
{
    return static_cast<uint32_t>((atomic & MID_BITS_MASK) >> COUNTER_WIDTH);
}

/**
 * Get the number of started jobs - 31 LSB
 * @param counter
 * @return num of started jobs
 */
uint32_t getNumStarted(uint64_t counter)
{

    return static_cast<uint32_t> (counter & LSB_BITS_MASK);
}

/**
 * Increment to the next stage and reset the count of jobs (started and finished)
 * @param counter
 */
void setStage(atomic64 *counter, stage_t stage)
{
    *counter = stage * 1ULL << COUNTER_WIDTH << COUNTER_WIDTH;
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
    JobContext *jobContext; //holds all other references
public:

    ThreadContext() : thread(), intermediate(), threadID(), jobContext()
    {}

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
        this->intermediate = std::move(intermediate);
    }

    /**
     * Copy attributes from another object into this one
     * @param other Another ThreadContext obj
     * @return
     */
    ThreadContext &operator=(const ThreadContext &other)
    {
        if (&other != this)
        {
            thread = other.thread;
            intermediate = other.intermediate;
            threadID = other.threadID;
            jobContext = other.jobContext;
        }
        return *this;
    }

    pthread_t &getThread()
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
class JobContext
{
private:
    ThreadContext *threads;
    const MapReduceClient &client;
    const InputVec &input;
    Barrier barrier; // to align all threads at a certain point
    sem_t shuffleSem; // to allow only a single thread in the shuffle stage
    sem_t stageSetSem; // safely set the stage from only one thread and avoid counter errors
    uint32_t multiThreadLevel; // how many threads
    std::vector<IntermediateVec> queue; // for reduce
    pthread_mutex_t joinMutex; // to join all threads and wait
    bool isJoined; // joining threads is allowed only one time
    OutputVec &output; // the output of the map reduce
    pthread_mutex_t emitMutex; //safely write from reduce
    size_t totalWork; // all work to to at a specific stage
    std::atomic<uint64_t> counter;

    // The main thread function - map, shuffle reduce and in between
    static void *runThread(void *threadContext);

    void static map(ThreadContext *tc);

    void shuffle(); // Doesn't need a thread context

    static void reduce(ThreadContext *tc);

    void sortIntermediates(IntermediateVec &tmpIntermediate) const;

    void joinSortedKeys(std::vector<IntermediatePair> &tmp);


public:

    /**
     * Construct a new map reduce job handler
     * @param client The client of the map and reduce actions. The client is expected to use emit2 in map
     * and emit3 in reduce.
     * @param inputVec input to map
     * @param outputVec output from reduce
     * @param multiThreadLevel how many threads to open
     */
    JobContext(const MapReduceClient &client,
               const InputVec &inputVec, OutputVec &outputVec,
               int multiThreadLevel);

    ~JobContext();

    /**
     * Wait for all workers. Calling this function will block progress until all threads finished
     */
    void joinContext();

    /**
     * Update the state input with the current progress
     */
    void getJobState(JobState *state);

    /**
     * Safely add a new key, value pair to the output during the reduce.
     * Shareable among threads
     */
    void writeToOutput(K3 *key, V3 *value);
};


JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    auto jc = new JobContext(client, inputVec, outputVec, multiThreadLevel);
    return jc;
}

/**
 * The function receives as input intermediary element (K2, V2) and context which contains
    data structure of the thread that created the intermediary element. The function saves the
    intermediary element in the context data structures.
 * @param key
 * @param value
 * @param context
 */
void emit2(K2 *key, V2 *value, void *context)
{

    auto *tc = static_cast<ThreadContext *>(context);
    tc->getIntermediate().push_back(IntermediatePair(key, value));
}

void emit3(K3 *key, V3 *value, void *context)
{

    auto *tc = static_cast<ThreadContext *>(context);
    tc->getJobContext()->writeToOutput(key, value);
}

void waitForJob(JobHandle job)
{
    auto jc = static_cast<JobContext *>(job);
    jc->joinContext();
}

void getJobState(JobHandle job, JobState *state)
{
    auto jc = static_cast<JobContext *>(job);
    jc->getJobState(state);
}

/**
 * Expects job to be allocated on the heap
 */
void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    auto jc = static_cast<JobContext *>(job);
    delete jc; //free resource
}

JobContext::JobContext(const MapReduceClient &client,
                       const InputVec &inputVec, OutputVec &outputVec,
                       int multiThreadLevel) : client(client), input(inputVec), barrier(multiThreadLevel),
                                               shuffleSem(), stageSetSem(),
                                               multiThreadLevel((uint32_t) multiThreadLevel),
                                               queue(), joinMutex(), isJoined(false),
                                               output(outputVec), emitMutex(), totalWork(input.size()), counter()
{
    threads = new(std::nothrow)ThreadContext[multiThreadLevel]();
    if (!threads)
    {
        error_exit("Memory allocation for threads array failed");
    }
    int ret = sem_init(&shuffleSem, 0, 0);
    ret |= sem_init(&stageSetSem, 0, 0);
    if (ret != SUCCESS)
    {
        error_exit("Semaphore initialization failed");
    }
    ret = pthread_mutex_init(&emitMutex, nullptr);
    ret |= pthread_mutex_init(&joinMutex, nullptr);
    if (ret != SUCCESS)
    {
        error_exit("Mutex initialization failed");
    }
    for (uint32_t i = 0; i < this->multiThreadLevel; i++)
    {
        pthread_t thread = 0;
        // Initialize all threads. Warning is not real since a bad alloc will exit
        threads[i] = ThreadContext(thread, IntermediateVec(), i, this);
        // Fun part starts here - spawn threads!!!!
        ret = pthread_create(&threads[i].getThread(), nullptr, &JobContext::runThread, (threads + i));
        if (ret != SUCCESS)
        {
            error_exit("Thread creation failed");
        }
    }
}

JobContext::~JobContext()
{
    delete[] threads;
    int ret = pthread_mutex_destroy(&emitMutex);
    ret |= pthread_mutex_destroy(&joinMutex);
    if (ret != SUCCESS)
    {
        error_exit("Mutex destruction returned an error");
    }
    ret = sem_destroy(&shuffleSem);
    ret |= sem_destroy(&stageSetSem);
    if (ret != SUCCESS)
    {
        error_exit("Semaphore destruction returned an error");
    }
}

void JobContext::getJobState(JobState *state)
{
    uint64_t counter_copy = counter.load(); //so it doesn't change during calculation
    state->percentage = ((float) getNumFinished(counter_copy) / (float) (totalWork)) * PERCENT_FACTOR;
    state->stage = static_cast<stage_t>(getStage(counter_copy));
}

void JobContext::writeToOutput(K3 *key, V3 *value)
{
    int ret = pthread_mutex_lock(&emitMutex);
    if (ret != SUCCESS)
    {
        error_exit(MUTEX_LOCK_ERROR);
    }
    output.push_back(OutputPair(key, value));
    ret = pthread_mutex_unlock(&emitMutex);
    if (ret != SUCCESS)
    {
        error_exit(MUTEX_UNLOCK_ERROR);
    }
}

void JobContext::joinContext()
{
    int ret = pthread_mutex_lock(&joinMutex); // Wait until the lock is released, probably until all threads join
    if (ret != SUCCESS)
    {
        error_exit(MUTEX_LOCK_ERROR);
    }
    if (!isJoined) // check if already joined
    {
        for (uint32_t i = 0; i < multiThreadLevel; i++)
        {
            int rett = pthread_join(threads[i].getThread(), nullptr);
            if (rett != SUCCESS)
            {
                error_exit("Error with joining threads");
            }
        }
        isJoined = true; // avoid a second join
    }
    ret = pthread_mutex_unlock(&joinMutex);
    if (ret != SUCCESS)
    {
        error_exit(MUTEX_UNLOCK_ERROR);
    }
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
    for (IntermediatePair pair:tmp)
    {
        counter++;
        if (!(*pair.first < *currPair.first) && !(*currPair.first < *pair.first))
        {
            matchingPairs.push_back(pair); //single key
        } else
        {
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
    for (uint32_t k = 0; k < multiThreadLevel; k++)
    {
        q_size += threads[k].getIntermediate().size();
    }
    tmpIntermediate.resize(q_size);
    size_t ind = 0;
    for (uint32_t k = 0; k < multiThreadLevel; k++)
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
        int ret = sem_wait(&j->shuffleSem); //stop all threads but the shuffling thread (zero)
        if (ret != SUCCESS)
        {
            error_exit(SEMAPHORE_WAIT_ERROR);
        }
    } else
    {
        j->shuffle();
    }
    int ret = sem_post(&j->shuffleSem); //resume threads, one after another
    if (ret != SUCCESS)
    {
        error_exit(SEMAPHORE_POST_ERROR);
    }
    reduce(tc);
    return nullptr;
}

void JobContext::map(ThreadContext *tc)
{
    JobContext *j = tc->getJobContext();
    if (tc->getThreadId() != SHUFFLE_THREAD_ID)
    {
        int res = sem_wait((&j->stageSetSem));
        if (res != SUCCESS)
        {
            error_exit(SEMAPHORE_WAIT_ERROR);
        }
    } else
    {
        setStage(&j->counter, MAP_STAGE);
    }
    int res = sem_post(&j->stageSetSem);
    if (res != SUCCESS)
    {
        error_exit(SEMAPHORE_POST_ERROR);
    }
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
    auto *j = (tc->getJobContext());
    auto k = getNumStarted(j->counter++);
    while (k < j->queue.size()) //atomically increase an index and get its previous value
    {
        const IntermediateVec *item = &j->queue[k];
        j->client.reduce(item, tc);
        k = getNumStarted(j->counter++);
        j->counter += 1ULL << COUNTER_WIDTH;
    }
}
