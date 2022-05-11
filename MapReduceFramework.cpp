/**
 * AUTHORS: Yitzchak Lindenbaum and Elay Aharoni
 * Implementation of MapReduceFramework library.
 * OS 2022 Spring Semester, Exercise 3
 */

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

#define LOAD_TOTAL(atomic_counter) ((atomic_counter->load() >> 31) & 0x7FFFFFFF)

#define LEFTMOST_2_BITS(num) (num >> 62)
#define RIGHTMOST_31_BITS(num) (uint64_t)num & 0x7FFFFFFF
#define MIDDLE_31_BITS(num) ((num >> 31) & 0x7FFFFFFF)


#define MUTEX_LOCK_ERR "pthread_mutex_lock"
#define MUTEX_UNLOCK_ERR "pthread_mutex_unlock"
#define PT_CREATE_ERR "pthread_create"
#define PT_JOIN_ERR "pthread_join"

#define CHECK(sysCall, errMsg) if (sysCall) { \
                        std::cerr << "system error: " << errMsg << std::endl; \
                        exit(1); \
                        }

#define NUM_MUTEXES 2
#define QUEUE_MUTEX_IND 0
#define WAIT_MUTEX_IND 1

// ================= DECLARATIONS ====================
struct ThreadTracker typedef ThreadTracker;
struct ThreadContext typedef ThreadContext;
struct JobHandleReal typedef JobHandleReal;

class IntPairComparator;

bool comparePairs(IntermediatePair a, IntermediatePair b);

bool equalPairs(IntermediatePair a, IntermediatePair b);

void *entry_point(void *arg);

void threadMap(ThreadContext * t_context);

IntermediatePair popFromSet(std::set<IntermediatePair, IntPairComparator> &shuffleSet);

void initShuffle(ThreadContext * t_context, std::set<IntermediatePair, IntPairComparator> & shuffleSet);
void threadShuffle(ThreadContext * t_context, std::set<IntermediatePair, IntPairComparator> & shuffleSet);
void threadReduce(ThreadContext * t_context);

// ================= IMPLEMENTATIONS ====================
/**
 * Holds all necessary information for a single thread.
 */
struct ThreadContext {
    int tid;
    const MapReduceClient *client;
    pthread_mutex_t *mutexes;
    const InputVec *inputVec;
    IntermediateVec intermediateVec;
    OutputVec *outputVec;
    std::atomic<uint64_t> *atomic_counter;
    Barrier *barrier;
    std::vector<IntermediateVec> *shuffleQueue;
    ThreadTracker *threadTracker; // will be NULL for all threads other than 0
} typedef ThreadContext;

/**
 * Holds necessary information to access all threads - to be passed to thread 0 only.
 */
struct ThreadTracker {
    struct ThreadContext *all_contexts;
    int num_threads;
} typedef ThreadTracker;

/**
 * Holds all information about an entire job.
 */
struct JobHandleReal {
    pthread_t *threads;
    ThreadTracker *threadTracker;
    bool done;
    pthread_mutex_t *mutexes;
};

/**
 * Comparator function for IntermediatePair type. Induces order relation of K2 type.
 * @param a
 * @param b
 * @return a < b ?
 */
bool comparePairs(const IntermediatePair a, const IntermediatePair b) {
    return *(a.first) < *(b.first);
}

/**
 * Equality checker for IntermediatePair type as defined by above comparator function.
 * @param a
 * @param b
 * @return a == b
 */
bool equalPairs(const IntermediatePair a, const IntermediatePair b) {
    return !comparePairs(a, b) && !comparePairs(b, a);
}

/**
 * Comparator class for IntermediatePair type, wraps above comparator function.
 */
class IntPairComparator {
public:
    bool operator()(const IntermediatePair a, const IntermediatePair b) const {
        return comparePairs(a, b);
    }
};

/**
 * Pops and returns back member of an ordered set.
 * @param shuffleSet
 * @return back member
 */
IntermediatePair popFromSet(std::set<IntermediatePair, IntPairComparator> &shuffleSet) {
    auto it = shuffleSet.end();
    it--;
    IntermediatePair to_place = *it;
    shuffleSet.erase(it);
    return to_place;
}

/**
 * Performs (a part of) the map stage on the input vector.
 * Note: each thread will call this function.
 */
void threadMap(ThreadContext * t_context) {
    auto num_pairs = t_context->inputVec->size();
    uint32_t old_value = RIGHTMOST_31_BITS((t_context->atomic_counter)->fetch_add(1));
    while (old_value < num_pairs) {
        K1 *key = t_context->inputVec->at(old_value).first;
        V1 *value = t_context->inputVec->at(old_value).second;
        t_context->client->map(key, value, t_context);
        old_value = RIGHTMOST_31_BITS((t_context->atomic_counter)->fetch_add(1));
    }
}

/**
 * Sets up atomic counter and shuffleSet for shuffle stage.
 * Note: to be called by thread 0 only.
 */
void initShuffle(ThreadContext * t_context, std::set<IntermediatePair, IntPairComparator> & shuffleSet) {
    auto atomic_counter = t_context->atomic_counter;
    SET_STAGE(atomic_counter, SHUFFLE_STAGE);
    const ThreadContext *all_contexts = t_context->threadTracker->all_contexts;
    int num_threads = t_context->threadTracker->num_threads;
    for (int i = 0; i < num_threads; ++i) {
        // total for shuffle stage is the sum of each thread's intermediateVec size.
        INC_TOTAL(atomic_counter, all_contexts[i].intermediateVec.size());
        if (!(all_contexts[i].intermediateVec.empty())) {
            // initialize set with backmost ("greatest") members of each intermediate vector.
            shuffleSet.insert(all_contexts[i].intermediateVec.back());
        }
    }
}

/**
 * Performs shuffle stage.
 * Note: to be called by thread 0 only.
 */
void threadShuffle(ThreadContext * t_context, std::set<IntermediatePair, IntPairComparator> & shuffleSet) {
    auto atomic_counter = t_context->atomic_counter;
    ThreadContext *all_contexts = t_context->threadTracker->all_contexts;
    int num_threads = t_context->threadTracker->num_threads;
    auto shuffleQueue = t_context->shuffleQueue;

    while (!shuffleSet.empty()) {
        IntermediatePair toPlace = popFromSet(shuffleSet);
        // since shuffleSet is sorted by induced key order, final member will always have a pair with the greatest K2
        IntermediateVec keyVec;
        for (int i = 0; i < num_threads; ++i) {
            IntermediateVec &curVec = all_contexts[i].intermediateVec;
            while (!(curVec.empty()) && equalPairs(toPlace, curVec.back()))
                // since toPlace has the greatest yet-unplaced key, and each intermediateVec is sorted, if there is a
                // pair with the same key in a thread's intermediateVec, it must be at the back.
                // Since there may be duplicates, continue taking final pairs until key has changed.
            {
                keyVec.push_back(curVec.back());
                curVec.pop_back();
                INC_PROGRESS(atomic_counter, 1);
            }
            // rebuild shuffleSet with new back members of each IntermediateVec
            if (!curVec.empty()) { shuffleSet.insert(curVec.back()); }
        }
        shuffleQueue->push_back(keyVec);
    }
}

/**
 * Perform reduce stage.
 * Note: to be called by each thread.
 */
void threadReduce(ThreadContext * t_context) {
    auto shuffleQueue = t_context->shuffleQueue;
    while (true) {
        // lock mutex to access shared shuffleQueue
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

/**
 * Begins map-reduce job
 * @param client
 * @param inputVec
 * @param outputVec
 * @param multiThreadLevel number of threads to initiate
 * @return struct containing all necessary information for handling job
 */
JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    pthread_t *threads = new pthread_t[multiThreadLevel];
    ThreadContext *t_contexts = new ThreadContext[multiThreadLevel];

    pthread_mutex_t *mutexes = new pthread_mutex_t[NUM_MUTEXES];
    for (int i = 0; i < NUM_MUTEXES; ++i) { mutexes[i] = PTHREAD_MUTEX_INITIALIZER; }

    ThreadTracker *threadTracker = new ThreadTracker();
    threadTracker->all_contexts = t_contexts;
    threadTracker->num_threads = multiThreadLevel;

    std::atomic<std::uint64_t> *atomic_counter = new std::atomic<std::uint64_t>(0);
    Barrier *barrier = new Barrier(multiThreadLevel);
    auto shuffleQueue = new std::vector<IntermediateVec>();

    SET_STAGE(atomic_counter, MAP_STAGE);
    INC_TOTAL(atomic_counter, inputVec.size());
    for (int i = 0; i < multiThreadLevel; ++i) {
        t_contexts[i].tid = i;
        t_contexts[i].client = &client;
        t_contexts[i].mutexes = mutexes;
        t_contexts[i].inputVec = &inputVec;
        t_contexts[i].outputVec = &outputVec;
        t_contexts[i].atomic_counter = atomic_counter;
        t_contexts[i].barrier = barrier;
        t_contexts[i].shuffleQueue = shuffleQueue;

        // Give thread 0 access to all threads
        if (i == 0) {
            t_contexts[i].threadTracker = threadTracker;
        } else { t_contexts[i].threadTracker = nullptr; }

        CHECK(pthread_create(threads + i, NULL, entry_point, t_contexts + i), PT_CREATE_ERR);
    }

    JobHandleReal *ret = new JobHandleReal{threads, threadTracker, false, mutexes};
    return ret;
}

// (Note: not technically a library function, but placed here for intuitive reasons)
/**
 * Function to send each thread to.
 * @param arg Will actually be ThreadContext
 * @return
 */
void *entry_point(void *arg) {

    ThreadContext *t_context = static_cast<ThreadContext *>(arg);

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
        auto atomic_counter_val = t_context->atomic_counter->load();
        uint32_t reduceTotal = MIDDLE_31_BITS(atomic_counter_val);
        SET_STAGE(t_context->atomic_counter, REDUCE_STAGE);
        INC_TOTAL(t_context->atomic_counter, reduceTotal);
    }

    t_context->barrier->barrier();

    // ================ REDUCE STAGE ================
    threadReduce(t_context);

    return nullptr;
}

/**
 * Place IntermediatePair in given ThreadContext's IntermediateVec
 * @param key
 * @param value
 * @param context
 * Note: a mutex is unnecessary here, since each thread has its own IntermediateVec.
 */
void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *t_context = static_cast<ThreadContext *>(context);
    t_context->intermediateVec.push_back(IntermediatePair(key, value));
}

/**
 * Place OutputPair in job's OutputVec
 * @param key
 * @param value
 * @param context
 * Note: a mutex is unnecessary here, since function is called by reduce function, which in turn is only called under
 * mutex protection.
 */
void emit3(K3 *key, V3 *value, void *context) {
    ThreadContext *t_context = static_cast<ThreadContext *>(context);
    t_context->outputVec->push_back(OutputPair(key, value));
}

/**
 * Wait and return only when job is fully done.
 * @param job
 */
void waitForJob(JobHandle job) {
    JobHandleReal *realJob = static_cast<JobHandleReal *>(job);

    int num_threads = realJob->threadTracker->num_threads;
    pthread_t *threads = realJob->threads;
    pthread_mutex_t *mutex = (realJob->mutexes) + WAIT_MUTEX_IND;

    // Lock mutex in case multiple threads try to call function in parallel - this is to prevent multiple calls to
    // pthread_join.
    CHECK(pthread_mutex_lock(mutex), MUTEX_LOCK_ERR);

    if (realJob->done) { // function has already been called once
        CHECK(pthread_mutex_unlock(mutex), MUTEX_UNLOCK_ERR);
        return;
    }

    for (int i = 0; i < num_threads; ++i) {
        CHECK(pthread_join(threads[i], NULL), PT_JOIN_ERR);
    }

    realJob->done = true;
    CHECK(pthread_mutex_unlock(mutex), MUTEX_UNLOCK_ERR);
}

/**
 * Place current stage and progress percentage in given state arg.
 * @param job
 * @param state
 */
void getJobState(JobHandle job, JobState *state) {
    JobHandleReal *realJob = static_cast<JobHandleReal *>(job);

    //state->stage = static_cast<stage_t>(LOAD_STAGE(atomic_counter));
    auto atomic_counter_val = realJob->threadTracker->all_contexts->atomic_counter->load();
    uint64_t stageNum = LEFTMOST_2_BITS(atomic_counter_val);
    stage_t stage = static_cast<stage_t>(stageNum);
    if (stage == UNDEFINED_STAGE) {
        state->stage = UNDEFINED_STAGE;
        state->percentage = 0;
        return;
    }

    uint64_t progress = RIGHTMOST_31_BITS(atomic_counter_val);
    uint64_t total = MIDDLE_31_BITS(atomic_counter_val);

    if (total == 0) { // at stage transition
        state->stage = stage;
        state->percentage = 0;
        return;
    }

    float percentage = std::min(1.0f, (float) ((double) progress / total)) * 100;
    state->stage = stage;
    state->percentage = percentage;
}

/**
 * Deallocates/destroys all resources created during job.
 * @param job
 */
void closeJobHandle(JobHandle job) {
    // Make sure job has ended first
    waitForJob(job);

    JobHandleReal *realJob = static_cast<JobHandleReal *>(job);
    delete realJob->threadTracker->all_contexts->atomic_counter;

    for (int i = 0; i < NUM_MUTEXES; ++i) { pthread_mutex_destroy(realJob->threadTracker->all_contexts->mutexes + i); }

    delete[] realJob->threadTracker->all_contexts->mutexes;
    delete realJob->threadTracker->all_contexts->shuffleQueue;
    delete realJob->threadTracker->all_contexts->barrier;
    delete[] realJob->threadTracker->all_contexts; // will this work?
    delete realJob->threadTracker;
    delete[] realJob->threads;
    delete realJob;
}

