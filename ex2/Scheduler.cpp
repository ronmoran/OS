//
// Created by user on 24/04/2021.
//
//todo remove
#include <utility>
#include <stdio.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <sys/time.h>
#include <queue>
#include <unordered_map>
#include <forward_list>
#include <iostream>
#include <mutex>
#include <algorithm>
#include <signal.h>
#include <queue>

#define SECOND 1000000
#define MAIN_THREAD 0
#define SUCCESS 0
#define FAILURE -1
#define MAIN_THREAD 0
#define UNLOCKED -1
#ifdef __x86_64__
/* code for 64 bit Intel arch */
#define JB_SP 6
#define JB_PC 7
typedef unsigned long address_t;

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr) {
    address_t ret;
    asm volatile("xor    %%fs:0x30,%0\n"
                 "rol    $0x11,%0\n"
    : "=g" (ret)
    : "0" (addr));
    return ret;
}

#else
/* code for 32 bit Intel arch */

typedef unsigned long address_t;
#define JB_SP 4
#define JB_PC 5

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%gs:0x18,%0\n"
        "rol    $0x9,%0\n"
                 : "=g" (ret)
                 : "0" (addr));
    return ret;
}

#endif


#include <stdio.h>
#include <signal.h>
#include <sys/time.h>

void timer_handler(int sig);


class Thread {
private:
    int id;
    bool blocked;
    bool mutexLocked;
    char *st;
    sigjmp_buf env;
    int numQuantum;

public:
    Thread() : id(), blocked(), mutexLocked(), st(nullptr), env() , numQuantum(0){

    };

    //Main thread ctor
    Thread(int threadId) : id(threadId), blocked(false), mutexLocked(), st(nullptr), env(), numQuantum(1) {
        sigemptyset(&env->__saved_mask);
    }

    explicit Thread(int threadId, void (*f)(void)) : id(threadId), blocked(), mutexLocked(), env(), numQuantum(0) {
        st = new(std::nothrow) char[STACK_SIZE]();
        if (!st) {
            std::cerr << "Alloc failed" << std::endl;
            exit(1);
        }
        address_t sp, pc;
        sp = (address_t) st + STACK_SIZE - sizeof(address_t);
        pc = (address_t) f;
        sigsetjmp(env, 1);
        (env->__jmpbuf)[JB_SP] = translate_address(sp);
        (env->__jmpbuf)[JB_PC] = translate_address(pc);
        /*todo really necessary?*/
        sigemptyset(&env->__saved_mask);
    }

    ~Thread() {
        if (st != nullptr) {
            delete[] st;
        }
    }

    Thread &operator=(const Thread &rhs) {
        if (&rhs == this) {
            return *this;
        }
        id = rhs.id;
        blocked = rhs.blocked;
        mutexLocked = rhs.mutexLocked;
        numQuantum = rhs.numQuantum;
        if (id == MAIN_THREAD) {
            sigemptyset(&env->__saved_mask);
            return *this;
        }
        st = new(std::nothrow) char[STACK_SIZE]; //todo should copy stack content?
        if (!st) {
            std::cerr << "Alloc failed" << std::endl;
            exit(1);
        }
        address_t sp;
        sp = (address_t) st + STACK_SIZE - sizeof(address_t);
        sigsetjmp(env, 1);
        (env->__jmpbuf)[JB_SP] = translate_address(sp); //todo std::copy for env?
        (env->__jmpbuf)[JB_PC] = rhs.env->__jmpbuf[JB_PC];
        sigemptyset(&env->__saved_mask);
        return *this;

    }

    int getNumQuantum() const {
        return numQuantum;
    }

    void block() {
        blocked = true;
    }

    void unBlock() {
        blocked = false;
    }

    bool isBlocked() const{
        return blocked;
    }

    bool isMutexLocked() const
    {
        return mutexLocked;
    }

    void mutexLock()
    {
        mutexLocked = true;
    }

    void mutexUnlock()
    {
        mutexLocked = false;
    }

    bool isReady() const
    {
        return !(blocked || mutexLocked);
    }

    sigjmp_buf *getEnv() {
        return &env;
    }


    void incQuantum() {
        numQuantum++;
    }
};

class Scheduler {
private:

    static int running;
    static int quantum;
    static int threadId;
    static std::vector<int> ready;
    static std::unordered_map<int, Thread> threads;
    static struct sigaction sa;
    static struct itimerval timer;
    static sigset_t set;
    static std::priority_queue<int, std::vector<int>, std::greater<int>> generateIds;
    static int mutexOwner;
    static std::vector<int> mutexWait;

    static void blockSignals();

    static void releaseSignals();

    static void removeFromReady(int tid);

    static void spawnMain();

    static void addToReady(int tid);

    static int popReady();



public:

    static int getThreadCount() {
        return threads.size();
    }
    static void switchThreads(bool terminate = false);

    static void initScheduler(int usecs);

    static int spawnThread(void (*f)(void));

    static int terminateThread(int threadId);

    static int blockThread(int tid, bool isMutexLock);

    static int getRunning();

    static int resumeThread(int tid);

    static int getQuantum();

    static int getThreadQuantum(int tid);

    static int mutexLock();

    static int mutexUnlock();
    static int getAvailableId();

    static void recycleId(int id);

    static void blockHelper(bool isMutexLock, Thread &curThread);
};

int Scheduler::quantum = 0;
int Scheduler::running = 0;
int Scheduler::threadId = 1;
std::vector<int> Scheduler::ready = std::vector<int>();
std::vector<int> Scheduler::mutexWait = std::vector<int>();
std::unordered_map<int, Thread> Scheduler::threads = std::unordered_map<int, Thread>();
std::priority_queue<int, std::vector<int>, std::greater<int>> Scheduler::generateIds = std::priority_queue<int, std::vector<int>, std::greater<int>>();
//std::mutex Scheduler::shared;
sigset_t Scheduler::set = {};
struct sigaction Scheduler::sa = {};
struct itimerval Scheduler::timer = {};
int Scheduler::mutexOwner = UNLOCKED;

void Scheduler::initScheduler(int usecs) {
    spawnMain();
    Scheduler::quantum = 1;
    int sec = usecs / 1000000;
    int usec = usecs % 1000000;
    Scheduler::running = MAIN_THREAD;
    sa.sa_handler = &timer_handler;
    timer.it_value.tv_usec = usec;
    timer.it_value.tv_sec = sec;
    timer.it_interval.tv_usec = usec;
    timer.it_interval.tv_sec = sec;
    if (setitimer(ITIMER_VIRTUAL, &timer, NULL)) {
        //todo change to cerr
        printf("setitimer error.");
    }
    if (sigaction(SIGVTALRM, &sa, NULL) < 0) {
        printf("sigaction error.");
    }
}

int Scheduler::spawnThread(void (*f)(void)) {
    blockSignals();
    int id = getAvailableId();
    Thread t(id, f);
    Scheduler::threads[id] = t;
    addToReady(id);
    releaseSignals();
    return id;
}


void Scheduler::spawnMain() {
    Scheduler::threads[MAIN_THREAD] = Thread(MAIN_THREAD);
}


void Scheduler::releaseSignals() {
    sigprocmask(SIG_UNBLOCK, &set, nullptr);
}

void Scheduler::blockSignals() {
    sigemptyset(&set);
    sigaddset(&set, SIGVTALRM);
    sigprocmask(SIG_BLOCK, &set, nullptr);
}


int Scheduler::terminateThread(int threadId) {
    Scheduler::blockSignals();
    if(mutexOwner == running)
    {
        mutexUnlock();
    }
    if (threadId != MAIN_THREAD) {
        if (Scheduler::threads.find(threadId) == Scheduler::threads.end()) {
            releaseSignals();
            return FAILURE;
        }
        if (Scheduler::running != threadId) {
            Scheduler::threads.erase(threadId);
            Scheduler::removeFromReady(threadId);
            recycleId(threadId);
        } else {
            recycleId(threadId);
            Scheduler::switchThreads(true);
        }
    } else {
        Scheduler::threads.clear();
        exit(SUCCESS);
    }

    releaseSignals();
    return SUCCESS;
}

void Scheduler::removeFromReady(int tid) {
    auto position = std::find(ready.begin(), ready.end(), tid);
    if (position != ready.end()) {
        ready.erase(position);
    }
}

void Scheduler::switchThreads(bool terminate) {
    //todo empty queue
    blockSignals();
    if (!terminate) {
        addToReady(running);
    }
    int current = popReady();
    int ret = 0;
    if (!terminate) {
        Thread &thread = threads[current];
        ret = sigsetjmp(*thread.getEnv(), 1);
    }
    if (ret == 0) {
        Thread &jump = threads[running];
        auto buf = jump.getEnv();
        quantum++;
        jump.incQuantum();
        if(terminate)
        {
            threads.erase(current);
        }
        if (setitimer(ITIMER_VIRTUAL, &timer, NULL)) {
            printf("setitimer error.");
        }
        releaseSignals(); //todo really after setitimer?
        siglongjmp(*buf, 1);
    }
    releaseSignals();
}

int Scheduler::popReady()
{
    int current = running;
    running = ready.front();
    ready.erase(ready.cbegin());
    return current;
}

int Scheduler::blockThread(int tid, bool isMutexLock)
{
    if (Scheduler::threads.find(tid) == Scheduler::threads.end() || tid == MAIN_THREAD) {
        return FAILURE;
    }
    blockSignals();
    Thread &curThread = threads[tid];
    if (tid == running) {
        blockHelper(isMutexLock, curThread);
        Scheduler::switchThreads();
    } else {
        Scheduler::removeFromReady(tid);
        blockHelper(isMutexLock, curThread);
    }
    releaseSignals();
    return SUCCESS;
}

void Scheduler::blockHelper(bool isMutexLock, Thread &curThread)
{
    if (isMutexLock)
    {
        curThread.mutexLock();
    }
    else
    {
        curThread.block();
    }
}


int Scheduler::getRunning() {
    return running;
}

void timer_handler(int) {
    Scheduler::switchThreads();
}


int Scheduler::resumeThread(int tid) {
    Scheduler::blockSignals();
    if (Scheduler::threads.find(tid) == Scheduler::threads.end()) {
        releaseSignals();
        return FAILURE;
    }
    Thread &thread = threads[tid];
    if(thread.isBlocked())
    {
        thread.unBlock();
        addToReady(tid);
    }
    releaseSignals();
    return SUCCESS;
}

void Scheduler::addToReady(int tid) // Assumes tid is in "threads"
{
    if (threads[tid].isReady())
    {
        ready.push_back(tid);
    }
}

int Scheduler::getQuantum() {
    return quantum;
}

int Scheduler::getThreadQuantum(int tid) {
    Scheduler::blockSignals();
    if (Scheduler::threads.find(tid) == Scheduler::threads.end()) {
        releaseSignals();
        return FAILURE;
    }
    const Thread &thread = Scheduler::threads[tid];

    int tmp = thread.getNumQuantum();
    releaseSignals();
    return tmp;
}

int Scheduler::mutexLock() {
    blockSignals();
    if(mutexOwner == running)
    {
        releaseSignals();
        return FAILURE;
    }
    while(mutexOwner != UNLOCKED)
    {
        mutexWait.push_back(running);
        blockThread(running, true); // signals are released here
        blockSignals();
    }
    mutexOwner = running;
    releaseSignals();
    return SUCCESS;
}

int Scheduler::getAvailableId()
{
    if (generateIds.empty()){
        return threadId++;
    }
    else{
        int id = generateIds.top();
        generateIds.pop();
        return id;
    }
}


void Scheduler::recycleId(int id){
    generateIds.push(id);
}


int Scheduler::mutexUnlock() {
    blockSignals();
    if (mutexOwner == UNLOCKED || mutexOwner != running)
    {
        releaseSignals();
        return FAILURE; //todo print error
    }
    mutexOwner = UNLOCKED;

//    auto tid = std::find_if(threads.begin(), threads.end(), [](std::pair<int,Thread> &p){return p.second.isMutexLocked()
//    ;});
//    (tid -> second).mutexUnlock();
//    addToReady(tid -> first);
//    for (auto &i: threads){
//        Thread &t = i.second;
//        if (t.isMutexLocked())
//        {
//            t.mutexUnlock();
//            addToReady(i.first);
//            break;
//        }
//    }
    if (!mutexWait.empty())
    {
        int tid = mutexWait.front();
        threads[tid].mutexUnlock();
        addToReady(tid);
        mutexWait.erase(mutexWait.cbegin());
    }
    releaseSignals();
    return SUCCESS;
}