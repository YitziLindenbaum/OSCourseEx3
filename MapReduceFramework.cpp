
#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <algorithm>
#include <semaphore.h>
#include <map>
#include <set>
#include "SampleClient.cpp"
#include "Barrier.h"

// =================== MACROS ======================
#define SET_STAGE(atomic_counter, stage) atomic_counter->store((uint64_t)stage << 62)
#define INC_PROGRESS(atomic_counter, to_add) atomic_counter->fetch_add(to_add)
#define INC_TOTAL(atomic_counter, to_add) atomic_counter->fetch_add((uint64_t)to_add << 31)

#define LOAD_STAGE(atomic_counter) atomic_counter->load() >> 62
#define LOAD_TOTAL(atomic_counter) ((atomic_counter->load() >> 31) & 0x7FFFFFFF)
#define SHIFT_PROGRESS(num) num & 0x7FFFFFFF

#define MUTEX_LOCK_ERR "pthread_mutex_lock"
#define MUTEX_UNLOCK_ERR "pthread_mutex_unlock"
#define PT_CREATE_ERR "pthread_create"
#define PT_JOIN_ERR "pthread_join"

#define CHECK(sysCall, errMsg) if (sysCall) { \
                        std::cerr << "system error: " << errMsg << std::endl; \
                        exit(1); \
                        }

#define NUM_MUTEXES 3
#define QUEUE_MUTEX_IND 0
#define WAIT_MUTEX_IND 1
#define STATE_MUTEX_IND 2

// ================= DECLARATIONS ====================
struct ThreadTracker typedef ThreadTracker;
struct ThreadContext typedef ThreadContext;
struct JobHandleReal typedef JobHandleReal;
class IntPairComparator;
bool comparePairs(IntermediatePair a, IntermediatePair b);
bool equalPairs(IntermediatePair a, IntermediatePair b);
void* entry_point(void* placeholder);
void threadMap(ThreadContext *t_context);
IntermediatePair popFromSet(std::set<IntermediatePair, IntPairComparator> &shuffleSet);
void initShuffle(ThreadContext *t_context, std::set<IntermediatePair, IntPairComparator> &shuffleSet);
void threadShuffle(ThreadContext *t_context, std::set<IntermediatePair, IntPairComparator> &shuffleSet);
void threadReduce(ThreadContext *t_context);

// ================= IMPLEMENTATIONS ====================
struct ThreadContext
{
    int tid;
    const MapReduceClient *client;
    pthread_mutex_t* mutexes;
    const InputVec *inputVec;
    IntermediateVec intermediateVec;
    OutputVec *outputVec;
    std::atomic<uint64_t> *atomic_counter;
    Barrier *barrier;
    std::vector<IntermediateVec> *shuffleQueue;
    ThreadTracker *threadTracker; // will be NULL for all threads other than 0
} typedef ThreadContext;

struct ThreadTracker {
    struct ThreadContext *all_contexts;
    int num_threads;
} typedef ThreadTracker;

struct JobHandleReal {
    pthread_t *threads;
    ThreadTracker *threadTracker;
    bool done;
    pthread_mutex_t* mutexes;
};

bool comparePairs(const IntermediatePair a, const IntermediatePair b) {
    return *(a.first) < *(b.first);
}

bool equalPairs(const IntermediatePair a, const IntermediatePair b) {
    return !comparePairs(a,b) && !comparePairs(b,a);
}

class IntPairComparator {
public:
    bool operator()(const IntermediatePair a, const IntermediatePair b) const {
        return comparePairs(a, b);
    }
};

IntermediatePair popFromSet(std::set<IntermediatePair, IntPairComparator> &shuffleSet)
{
    auto it = shuffleSet.end();
    it--;
    IntermediatePair to_place = *it;
    shuffleSet.erase(it);
    return to_place;
}

void threadMap(ThreadContext *t_context)
{
    auto num_pairs = t_context->inputVec->size();
    int old_value = SHIFT_PROGRESS((t_context->atomic_counter)->fetch_add(1));
    while (old_value < num_pairs)
    {
        K1 *key = t_context->inputVec->at(old_value).first;
        V1 *value = t_context->inputVec->at(old_value).second;
        t_context->client->map(key, value, t_context);
        old_value = SHIFT_PROGRESS((t_context->atomic_counter)->fetch_add(1));
    }
}

void initShuffle(ThreadContext *t_context, std::set<IntermediatePair, IntPairComparator> &shuffleSet)
{
    auto atomic_counter = t_context->atomic_counter;

    SET_STAGE(atomic_counter, SHUFFLE_STAGE);
    //usleep(100000);
    const ThreadContext *all_contexts = t_context->threadTracker->all_contexts;
    int num_threads = t_context->threadTracker->num_threads;
    for (int i = 0; i < num_threads; ++i) {

        INC_TOTAL(atomic_counter, all_contexts[i].intermediateVec.size());
        if (!(all_contexts[i].intermediateVec.empty()))
        {
            shuffleSet.insert(all_contexts[i].intermediateVec.back());
        }
    }
}

void threadShuffle(ThreadContext *t_context, std::set<IntermediatePair, IntPairComparator> &shuffleSet)
{
    auto atomic_counter = t_context->atomic_counter;
    ThreadContext *all_contexts = t_context->threadTracker->all_contexts;
    int num_threads = t_context->threadTracker->num_threads;
    auto shuffleQueue = t_context->shuffleQueue;

    while (!shuffleSet.empty()) {
        IntermediatePair toPlace = popFromSet(shuffleSet);

        IntermediateVec keyVec;
        for (int i = 0; i < num_threads; ++i) {
            IntermediateVec &curVec = all_contexts[i].intermediateVec;
            while (!(curVec.empty()) && equalPairs(toPlace, curVec.back()))
            {
                keyVec.push_back(curVec.back());
                curVec.pop_back();
                INC_PROGRESS(atomic_counter, 1);
                //usleep(100000);
            }
            if (!curVec.empty()) {shuffleSet.insert(curVec.back());}
        }
        shuffleQueue->push_back(keyVec);
    }
}

void threadReduce(ThreadContext *t_context) {
    auto shuffleQueue = t_context->shuffleQueue;
    while (true) {
        //printf("current progress: %lu\n", SHIFT_PROGRESS(t_context->atomic_counter->load()));
        CHECK(pthread_mutex_lock(t_context->mutexes + QUEUE_MUTEX_IND), MUTEX_LOCK_ERR);
        if (shuffleQueue->empty()) {
            CHECK(pthread_mutex_unlock(t_context->mutexes + QUEUE_MUTEX_IND), MUTEX_UNLOCK_ERR);
            break;
        }
        t_context->client->reduce(&(shuffleQueue->back()), t_context);
        INC_PROGRESS(t_context->atomic_counter, shuffleQueue->back().size());
        t_context->shuffleQueue->pop_back();
        CHECK(pthread_mutex_unlock(t_context->mutexes + QUEUE_MUTEX_IND), MUTEX_UNLOCK_ERR);
    }
}

// ================= IMPLEMENTATION OF MapReduceFramework LIBRARY ====================

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    pthread_t *threads = new pthread_t[multiThreadLevel];
    ThreadContext *t_contexts = new ThreadContext[multiThreadLevel];

    pthread_mutex_t *mutexes = new pthread_mutex_t[NUM_MUTEXES];
    for (int i = 0; i < NUM_MUTEXES; ++i) {mutexes[i] = PTHREAD_MUTEX_INITIALIZER;}

    ThreadTracker *threadTracker = new ThreadTracker();
    threadTracker->all_contexts = t_contexts;
    threadTracker->num_threads = multiThreadLevel;

    std::atomic<std::uint64_t> *atomic_counter = new std::atomic<std::uint64_t>(0);
    Barrier *barrier = new Barrier(multiThreadLevel);
    auto shuffledQueue = new std::vector<IntermediateVec>();

    SET_STAGE(atomic_counter, MAP_STAGE);
    INC_TOTAL(atomic_counter, inputVec.size());
    for (int i = 0; i < multiThreadLevel; ++i) // todo creat constructor ?(destructor)?
    {
        t_contexts[i].tid = i;
        t_contexts[i].client = &client;
        t_contexts[i].mutexes = mutexes;
        t_contexts[i].inputVec = &inputVec;
        t_contexts[i].outputVec = &outputVec;
        t_contexts[i].atomic_counter = atomic_counter;
        t_contexts[i].barrier = barrier;
        t_contexts[i].shuffleQueue = shuffledQueue;

        // Give thread 0 access to all threads
        if (i == 0) {
            t_contexts[i].threadTracker = threadTracker;
        } else{t_contexts[i].threadTracker = nullptr;}

        CHECK(pthread_create(threads + i, NULL, entry_point, t_contexts + i), PT_CREATE_ERR);
    }


    /*for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_join(threads[i], NULL);
    }*/




    //printf("total: %lu\n", LOAD_TOTAL(atomic_counter));
    //printf("final progress: %lu\n", SHIFT_PROGRESS(atomic_counter->load()));

    JobHandleReal *ret = new JobHandleReal{threads, threadTracker, false, mutexes};
    return ret;
    //=========
}

// (Note: not technically a library function, but placed here for intuitive reasons)
void *entry_point(void *placeholder)
{

    ThreadContext *t_context = static_cast<ThreadContext*>(placeholder);

    // ================ MAP STAGE ================

    threadMap(t_context);

    //================= SORT STAGE ===============

    auto vec_begin = t_context->intermediateVec.begin();
    auto vec_end = t_context->intermediateVec.end();
    std::sort(vec_begin, vec_end, comparePairs);

    t_context->barrier->barrier();


    // ================ SHUFFLE STAGE ================
    if (t_context->tid == 0) {
        std::set<IntermediatePair, IntPairComparator> shuffleSet;
        initShuffle(t_context, shuffleSet);
        threadShuffle(t_context, shuffleSet);

        // Set up for Reduce stage
        uint32_t reduceTotal = LOAD_TOTAL(t_context->atomic_counter);
        SET_STAGE(t_context->atomic_counter, REDUCE_STAGE);
        INC_TOTAL(t_context->atomic_counter, reduceTotal);
    }

    t_context->barrier->barrier();

    // ================ REDUCE STAGE ================
    threadReduce(t_context);


    //TODO change
    return nullptr;
    //===========
}

void emit2(K2 *key, V2 *value, void *context)
{
    ThreadContext *t_context = static_cast<ThreadContext *>(context);
    t_context->intermediateVec.push_back(IntermediatePair(key, value));
}

void emit3(K3 *key, V3 *value, void *context)
{
    // is using second mutex here necessary?
    ThreadContext *t_context = static_cast<ThreadContext *>(context);
    t_context->outputVec->push_back(OutputPair(key, value));
}

// TODO test
void waitForJob(JobHandle job)
{
    JobHandleReal *realJob = static_cast<JobHandleReal*>(job);
    int num_threads = realJob->threadTracker->num_threads;
    pthread_t *threads = realJob->threads;
    pthread_mutex_t* mutex = (realJob->mutexes) + WAIT_MUTEX_IND;

    CHECK(pthread_mutex_lock(mutex), MUTEX_LOCK_ERR);
    if (realJob->done) {
        CHECK(pthread_mutex_unlock(mutex), MUTEX_UNLOCK_ERR);
        return;
    }
    for (int i = 0; i < num_threads; ++i)
    {
        CHECK(pthread_join(threads[i], NULL), PT_JOIN_ERR);
    }
    realJob->done = true;
    CHECK(pthread_mutex_unlock(mutex), MUTEX_UNLOCK_ERR);
}

// todo test
void getJobState(JobHandle job, JobState *state)
{
    JobHandleReal *realJob = static_cast<JobHandleReal*>(job);
    auto atomic_counter = realJob->threadTracker->all_contexts->atomic_counter;

    //state->stage = static_cast<stage_t>(LOAD_STAGE(atomic_counter));
    stage_t stage = static_cast<stage_t>(LOAD_STAGE(atomic_counter));
    if (stage == UNDEFINED_STAGE) {
        JobState newState = {UNDEFINED_STAGE, 0};
        *state = newState;
        return;
    }

    double progress = SHIFT_PROGRESS(atomic_counter->load());
    double total = LOAD_TOTAL(atomic_counter); // use doubles to make sure 32 bit ints don't overflow
    //std::cout << "total: " << total << std::endl << "progress: " << progress << std::endl;
    //std::cout.flush();
    if (total == 0) { // at stage transition
        JobState newState = {stage, 0};
        *state = newState;
        return;
    }
    float percentage = std::min(1.0f, (float)(progress / total)) * 100;
    JobState newState = {stage, percentage};
    *state = newState;
}

void closeJobHandle(JobHandle job)
{
    // Make sure job has ended first
    waitForJob(job);

    JobHandleReal *realJob = static_cast<JobHandleReal*>(job);
    delete realJob->threadTracker->all_contexts->atomic_counter;
    delete[] realJob->threadTracker->all_contexts->mutexes;
    delete realJob->threadTracker->all_contexts->shuffleQueue;
    delete realJob->threadTracker->all_contexts->barrier;
    delete[] realJob->threadTracker->all_contexts; // will this work?
    delete realJob->threadTracker;
    delete[] realJob->threads;
    delete realJob;
}


/*int main(int argc, char **argv)
{
    CounterClient client;
    InputVec input_vec;
    OutputVec output_vec;
    VString s1("This string is full of characters");
    VString s2("Multithreading is awesome aa");
    VString s3("Race conditions are bad");
    input_vec.push_back({nullptr, &s1});
    input_vec.push_back({nullptr, &s2});
    input_vec.push_back({nullptr, &s3});

    // starting the program
    JobHandle job = startMapReduceJob(client, input_vec, output_vec, 2);

    waitForJob(job);

    std::cout << "[" ;
    for (const auto &item : output_vec)
    {
        std::cout << " (" << ((const KChar*)(item.first))->c <<
        " : "
        << ((const VCount*)(item.second))->count <<
        ")," << std::endl;
    }
    std::cout << "]" << std::endl;

}*/

