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

#define SECOND 1000000
#define STACK_SIZE 4096
#define MAIN_THREAD 0
#define JB_SP 4
#define JB_PC 5
typedef unsigned int address_t;


class Thread
{
private:
    const unsigned int id;
    bool isBlocked;
    sigjmp_buf env;
public:
    explicit Thread(unsigned int threadId, void (*f)(void)): id(threadId), isBlocked(false)
    {
        address_t sp, pc;
        char* stack = (char *)(malloc(sizeof(char) * STACK_SIZE));
        sp = (address_t)stack + STACK_SIZE - sizeof(address_t);
        pc = (address_t)f;
    }

};

class Scheduler
{
private:

    static unsigned int running;
    static unsigned int quantum;
    static unsigned int threadCount;
    static unsigned int threadId;
    static std::vector<int> ready;
    static std::unordered_map<unsigned int, Thread> threads;
    static std::mutex shared;
public:
    static int getThreadCount(){return threadCount;}
    static void initScheduler(int usecs);
    static unsigned int spawnThread(void (*f)(void));
};

unsigned int Scheduler::quantum = 0;
unsigned int Scheduler::running = 0;
unsigned int Scheduler::threadCount = 0;
unsigned int Scheduler::threadId = 0;
//std::vector<int> Scheduler:: ready = std::vector<int>();
//std::unordered_map<unsigned int, Thread> Scheduler:: threads = std::unordered_map<unsigned int, Thread>();

void Scheduler::initScheduler(int usecs)
{
    Scheduler::quantum = usecs;
    Scheduler::running = MAIN_THREAD;

}

unsigned int Scheduler::spawnThread(void (*f)(void))
{
    const std::lock_guard<std::mutex> lock(shared);

}
