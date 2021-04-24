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

#define MAIN_THREAD 0

class Thread
{
private:
    unsigned int id;
    bool isBlocked;
    sigjmp_buf env;
public:
    explicit Thread(unsigned int threadId)
    {
        isBlocked = false;
        id = threadId;
    }

};

class Scheduler
{
private:

    static unsigned int running;
    static std::forward_list<int> ready;
    static std::unordered_map<unsigned int, Thread> threads;
public:
    static unsigned int quantum;

    static void initScheduler(int usecs);
};

unsigned int Scheduler::quantum = 0;

void Scheduler::initScheduler(int usecs)
{
    Scheduler::quantum = usecs;
    running = MAIN_THREAD;


}
