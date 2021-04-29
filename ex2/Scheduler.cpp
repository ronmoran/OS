//
// Created by user on 24/04/2021.
//
//todo remove
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

#define SECOND 1000000
#define STACK_SIZE 4096
#define MAIN_THREAD 0
#define SUCCESS 0
#define FAILURE -1

#define MAIN_THREAD 0


#ifdef __x86_64__
/* code for 64 bit Intel arch */

typedef unsigned long address_t;
#define JB_SP 6
#define JB_PC 7

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
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

class Thread {
private:
    int id;
    bool blocked;
    sigjmp_buf env;
    char *st;
public:
    Thread() : id(), blocked(), env(), st(nullptr) {

    };

    //Main thread ctor
    Thread(int threadId) : id(threadId), blocked(false), st(nullptr), env() {
        sigemptyset(&env->__saved_mask);
    }

    explicit Thread(int threadId, void (*f)(void)) : id(threadId), blocked(false), env() {
        st = new(std::nothrow) char[STACK_SIZE];
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
        if(id == MAIN_THREAD)
        {
            sigemptyset(&env->__saved_mask);
            return *this;
        }
        st = new(std::nothrow) char[STACK_SIZE]; //todo should copy stack content?
        if (!st) {
            std::cerr << "Alloc failed" << std::endl;
            exit(1);
        }
        address_t sp, pc;
        sp = (address_t) st + STACK_SIZE - sizeof(address_t);
        sigsetjmp(env, 1);
        (env->__jmpbuf)[JB_SP] = translate_address(sp); //todo std::copy for env?
        (env->__jmpbuf)[JB_PC] = rhs.env->__jmpbuf[JB_PC];
        sigemptyset(&env->__saved_mask);
        return *this;

    }

    void block() {
        blocked = true;
    }

    void unblock() {
        blocked = false;
    }

    bool isBlocked() {
        return blocked;
    }

    sigjmp_buf* getEnv() {
        return &env;
    }


};

class Scheduler {
private:

    static int running;
    static unsigned int quantum;
    static int threadId;
    static std::vector<int> ready;
    static std::unordered_map<int, Thread> threads;
//    static std::mutex shared;
    static sigset_t set;

    static void blockSignals();

    static void releaseSignals();

    static void removeFromReady(int tid);

    static void spawnMain();


public:
    static void switchThreads();
    static int getThreadCount() { return threads.size(); }

    static void initScheduler(int usecs);

    static int spawnThread(void (*f)(void));

    static int terminateThread(int threadId);

    static int blockThread(int threadId);
};

unsigned int Scheduler::quantum = 0;
int Scheduler::running = 0;
int Scheduler::threadId = 1;
std::vector<int> Scheduler::ready = std::vector<int>();
std::unordered_map<int, Thread> Scheduler::threads = std::unordered_map<int, Thread>();
//std::mutex Scheduler::shared;
sigset_t Scheduler::set = {};

void Scheduler::initScheduler(int usecs) {
    spawnMain();
    Scheduler::quantum = usecs;
    Scheduler::running = MAIN_THREAD;


}

int Scheduler::spawnThread(void (*f)(void)) {
    blockSignals();
    Thread t(Scheduler::threadId, f);
    Scheduler::threads[Scheduler::threadId] = t;
    Scheduler::ready.push_back(threadId);
    int tmp = Scheduler::threadId++;
    releaseSignals();
    return tmp;
}


void Scheduler::spawnMain() {
    Scheduler::threads[MAIN_THREAD] = Thread(MAIN_THREAD);
}


void Scheduler::releaseSignals() { sigprocmask(SIG_UNBLOCK, &set, nullptr); }

void Scheduler::blockSignals() {
    sigemptyset(&set);
    sigfillset(&set);
    sigprocmask(SIG_BLOCK, &set, nullptr);
}


int Scheduler::terminateThread(int threadId) {
    Scheduler::blockSignals();
    if (threadId != 0) {
        if (Scheduler::threads.find(threadId) == Scheduler::threads.end()) {
            releaseSignals();
            return FAILURE;
        }
        if (Scheduler::running != threadId) {
            Scheduler::threads.erase(threadId);
            Scheduler::removeFromReady(threadId);
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

int Scheduler::blockThread(int tid) {
    if (Scheduler::threads.find(tid) == Scheduler::threads.end() || tid == MAIN_THREAD) {
        return FAILURE;
    }
    blockSignals();
    if (tid == running) {
        threads[tid].block();


    } else {
        Scheduler::removeFromReady(threadId);
        threads[tid].block();
    }
    releaseSignals();
}

void Scheduler::switchThreads() {
    blockSignals();
    if (!threads[running].isBlocked()) {
        ready.push_back(running);
    }
    int current = running;
    running = ready.front();
    ready.erase(ready.cbegin());
    int ret = sigsetjmp(*threads[current].getEnv(), 1);
    if (ret == 0) {
        auto buf = threads[running].getEnv();
        siglongjmp(*buf, 1);
    }
    releaseSignals();
}

