#include "Scheduler.cpp"
#define ERR_MSG "thread library error: "



/*
 * Description: This function initializes the thread library.
 * You may assume that this function is called before any other thread library
 * function, and that it is called exactly once. The input to the function is
 * the length of a quantum in micro-seconds. It is an error to call this
 * function with non-positive quantum_usecs.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_init(int quantum_usecs){
    if (quantum_usecs <= 0){
        std::cerr << ERR_MSG << "Cannot enter negative quantum" << std::endl;
        return FAILURE;
    }
    Scheduler::initScheduler(quantum_usecs);
    return SUCCESS;
}




/*
 * Description: This function creates a new thread, whose entry point is the
 * function f with the signature void f(void). The thread is added to the end
 * of the READY threads list. The uthread_spawn function should fail if it
 * would cause the number of concurrent threads to exceed the limit
 * (MAX_THREAD_NUM). Each thread should be allocated with a stack of size
 * STACK_SIZE bytes.
 * Return value: On success, return the ID of the created thread.
 * On failure, return -1.
*/
int uthread_spawn(void (*f)()){
    if(Scheduler::getThreadCount() >= MAX_THREAD_NUM) {
        std::cerr << ERR_MSG << "Cannot spawn new threads, reached max count" << std::endl;
        return FAILURE;
    }
    return Scheduler::spawnThread(f);
}



/*
 * Description: This function terminates the thread with ID tid and deletes
 * it from all relevant control structures. All the resources allocated by
 * the library for this thread should be released. If no thread with ID tid
 * exists it is considered an error. Terminating the main thread
 * (tid == 0) will result in the termination of the entire process using
 * exit(0) [after releasing the assigned library memory].
 * Return value: The function returns 0 if the thread was successfully
 * terminated and -1 otherwise. If a thread terminates itself or the main
 * thread is terminated, the function does not return.
*/
int uthread_terminate(int tid){
    int val = Scheduler::terminateThread(tid);
    if (val==FAILURE) {
        std::cerr << ERR_MSG << "tid not found" << std::endl;
    }
    return val;
}



/*
 * Description: This function blocks the thread with ID tid. The thread may
 * be resumed later using uthread_resume. If no thread with ID tid exists it
 * is considered as an error. In addition, it is an error to try blocking the
 * main thread (tid == 0). If a thread blocks itself, a scheduling decision
 * should be made. Blocking a thread in BLOCKED state has no
 * effect and is not considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_block(int tid){
    if (tid == MAIN_THREAD)
    {
        std::cerr << ERR_MSG << "Cannot block main thread" << std::endl;
        return FAILURE;
    }
    int val = Scheduler::blockThread(tid, false);
    if(val == FAILURE) {
        std::cerr << ERR_MSG << "thread not found" << std::endl;
    }
    return val;
}


/*
 * Description: This function resumes a blocked thread with ID tid and moves
 * it to the READY state if it's not synced. Resuming a thread in a RUNNING or READY state
 * has no effect and is not considered as an error. If no thread with
 * ID tid exists it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_resume(int tid){
    int val = Scheduler::resumeThread(tid);
    if(val==FAILURE)
    {
        std::cerr << ERR_MSG << "thread not found" << std::endl;
    }
    return val;
}


/*
 * Description: This function tries to acquire a mutex.
 * If the mutex is unlocked, it locks it and returns.
 * If the mutex is already locked by different thread, the thread moves to BLOCK state.
 * In the future when this thread will be back to RUNNING state,
 * it will try again to acquire the mutex.
 * If the mutex is already locked by this thread, it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_mutex_lock(){
    int val = Scheduler::mutexLock();
    if(val==FAILURE)
    {
        std::cerr << ERR_MSG << "Cannot lock when mutex already acquired" << std::endl;
    }
    return val;
}


/*
 * Description: This function releases a mutex.
 * If there are blocked threads waiting for this mutex,
 * one of them (no matter which one) moves to READY state.
 * If the mutex is already unlocked, it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_mutex_unlock(){
    int val = Scheduler::mutexUnlock();
    if(val==FAILURE)
    {
        std::cerr << ERR_MSG << "Thread is not locked";
    }
    return val;
}


/*
 * Description: This function returns the thread ID of the calling thread.
 * Return value: The ID of the calling thread.
*/
int uthread_get_tid(){
    return Scheduler::getRunning();
}


/*
 * Description: This function returns the total number of quantums since
 * the library was initialized, including the current quantum.
 * Right after the call to uthread_init, the value should be 1.
 * Each time a new quantum starts, regardless of the reason, this number
 * should be increased by 1.
 * Return value: The total number of quantums.
*/
int uthread_get_total_quantums(){
    return Scheduler::getQuantum();
}


/*
 * Description: This function returns the number of quantums the thread with
 * ID tid was in RUNNING state. On the first time a thread runs, the function
 * should return 1. Every additional quantum that the thread starts should
 * increase this value by 1 (so if the thread with ID tid is in RUNNING state
 * when this function is called, include also the current quantum). If no
 * thread with ID tid exists it is considered an error.
 * Return value: On success, return the number of quantums of the thread with ID tid.
 * 			     On failure, return -1.
*/
int uthread_get_quantums(int tid){
    int val= Scheduler::getThreadQuantum(tid);
    if(val==FAILURE)
    {
        std::cerr << ERR_MSG << "thread not found" << std::endl;
    }
    return val;
}