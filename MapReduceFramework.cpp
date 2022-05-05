
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
#define RESET_PROGRESS(atomic_counter) atomic_counter->store((uint64_t)new_total << 31)
#define INC_TOTAL(atomic_counter, to_add) atomic_counter->fetch_add((uint64_t)to_add << 31)

#define LOAD_STAGE(num) num >> 62
#define LOAD_TOTAL(num) ((num >> 31) & 0x7FFFFFFF)
#define LOAD_PROGRESS(num) num & 0x7FFFFFFF

#define NUM_MUTEXES 2
#define QUEUE_MUTEX 0
#define OUTPUT_MUTEX 1

// ================= DECLARATIONS ====================
struct ThreadTracker typedef ThreadTracker;
struct ThreadContext typedef ThreadContext;
class IntPairComparator;
bool comparePairs(const IntermediatePair a, const IntermediatePair b);
bool equalPairs(const IntermediatePair a, const IntermediatePair b);
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

void threadShuffle(ThreadContext *t_context, std::set<IntermediatePair, IntPairComparator> &shuffleSet)
{
    auto atomic_counter = t_context->atomic_counter;
    ThreadContext *all_contexts = t_context->threadTracker->all_contexts;
    int num_threads = t_context->threadTracker->num_threads;
    auto shuffleQueue = t_context->shuffleQueue;

    while (!shuffleSet.empty()) {
        IntermediatePair toPlace = popFromSet(shuffleSet);

        IntermediateVec *keyVec = new IntermediateVec();
        for (int i = 0; i < num_threads; ++i) {
            IntermediateVec &curVec = all_contexts[i].intermediateVec;
            while (!(curVec.empty()) && equalPairs(toPlace, curVec.back()))
            {
                keyVec->push_back(curVec.back());
                curVec.pop_back();
                INC_PROGRESS(atomic_counter, 1);
            }
            if (!curVec.empty()) {shuffleSet.insert(curVec.back());}
        }
        shuffleQueue->push_back(*keyVec);
    }
}

void initShuffle(ThreadContext *t_context, std::set<IntermediatePair, IntPairComparator> &shuffleSet)
{
    auto atomic_counter = t_context->atomic_counter;
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
    int num_pairs = t_context->inputVec->size();
    int old_value = LOAD_PROGRESS((t_context->atomic_counter)->fetch_add(1));
    while (old_value < num_pairs)
    {
        K1 *key = t_context->inputVec->at(old_value).first;
        V1 *value = t_context->inputVec->at(old_value).second;
        t_context->client->map(key, value, t_context);
        old_value = LOAD_PROGRESS((t_context->atomic_counter)->fetch_add(1));
    }
}

void threadReduce(ThreadContext *t_context) {
    auto shuffleQueue = t_context->shuffleQueue;
    while (true) {
        printf("current progress: %llu\n", LOAD_PROGRESS(t_context->atomic_counter->load()));
        pthread_mutex_lock(t_context->mutexes + QUEUE_MUTEX);
        if (shuffleQueue->empty()) {
            pthread_mutex_unlock(t_context->mutexes + QUEUE_MUTEX);
            break;
        }
        t_context->client->reduce(&(shuffleQueue->back()), t_context);
        INC_PROGRESS(t_context->atomic_counter, shuffleQueue->back().size());
        t_context->shuffleQueue->pop_back();
        pthread_mutex_unlock(t_context->mutexes + QUEUE_MUTEX);
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

    INC_TOTAL(atomic_counter, inputVec.size());
    SET_STAGE(atomic_counter, MAP_STAGE);
    for (int i = 0; i < multiThreadLevel; ++i) // todo export to function
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
        }

        pthread_create(threads + i, NULL, entry_point, t_contexts + i);
    }


    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_join(threads[i], NULL);
    }


    //todo remove
    /*for (int i = 0; i < multiThreadLevel; ++i)
    {
        std::cout << "intermediate_vec for thread " << i << " is: \n [ ";
        for (int j = 0; j < t_contexts[i].intermediateVec.size(); ++j)
        {

            std::cout << "(" << ((const KChar *) (t_contexts[i].intermediateVec.at(j).first))->c <<
                      " : "
                      << ((const VCount *) (t_contexts[i].intermediateVec.at(j).second))->count <<
                      ") " << "," << std::endl;
        }
        std::cout << "]" << std::endl;
    }*/
    //==========

    std::cout << "[" ;
    for (const auto &item : outputVec)
    {
        std::cout << " (" << ((const KChar*)(item.first))->c <<
                  " : "
                  << ((const VCount*)(item.second))->count <<
                  ")," << std::endl;
    }
    std::cout << "]" << std::endl;

    printf("total: %llu\n", LOAD_TOTAL(atomic_counter->load()));
    printf("final progress: %llu\n", LOAD_PROGRESS(atomic_counter->load()));

    //TODO change
    return atomic_counter;
    //=========
}


// (Note: not technically a library function, but placed here for intuitive reasons)
void *entry_point(void *placeholder)
{

    ThreadContext *t_context = static_cast<ThreadContext*>(placeholder);

    // ================ MAP STAGE ================

    threadMap(t_context);

    //            ======= SORT =======

    auto vec_begin = t_context->intermediateVec.begin();
    auto vec_end = t_context->intermediateVec.end();
    std::sort(vec_begin, vec_end, comparePairs);

    t_context->barrier->barrier();


    // ================ SHUFFLE STAGE ================
    if (t_context->tid == 0) {
        // set up Shuffle stage
        t_context->atomic_counter->store(0);
        SET_STAGE(t_context->atomic_counter, SHUFFLE_STAGE); //TODO check forum about when exactly to change stage
        // do shuffle

        std::set<IntermediatePair, IntPairComparator> shuffleSet;

        // Place "greatest" pairs in each thread's vector in ordered set
        initShuffle(t_context, shuffleSet);

        threadShuffle(t_context, shuffleSet);

        /*for (const auto &keyVec : *(t_context->shuffleQueue)) {
            std::cout << "new vector" << std::endl;
            std::cout << "[" ;
            for (const auto &item : keyVec)
            {
                std::cout << " (" << ((const KChar*)(item.first))->c <<
                          " : "
                          << ((const VCount*)(item.second))->count <<
                          ")" << ",";
            }
            std::cout << "]" << std::endl;
        }*/
        // todo remove
        //printf("total: %llu\n", LOAD_TOTAL(t_context->atomic_counter->load()));
        //printf("progress: %llu\n", LOAD_PROGRESS(t_context->atomic_counter->load()));

        // Set up for Reduce stage
        uint32_t reduceTotal = LOAD_TOTAL(t_context->atomic_counter->load());
        t_context->atomic_counter->store(0);
        SET_STAGE(t_context->atomic_counter, REDUCE_STAGE); //TODO check forum about when exactly to change stage
        INC_TOTAL(t_context->atomic_counter, reduceTotal);
    }

    t_context->barrier->barrier();

    // ================ REDUCE STAGE ================
    threadReduce(t_context);

    //TODO change
    return t_context->atomic_counter;
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


void waitForJob(JobHandle job)
{

}


void getJobState(JobHandle job, JobState *state)
{

}


void closeJobHandle(JobHandle job)
{

}


int main(int argc, char **argv)
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


}



//int main(int argc, char** argv)
//{
//  CounterClient client;
//  InputVec inputVec;
//  OutputVec outputVec;
//  VString s1("This string is full of characters");
//  VString s2("Multithreading is awesome");
//  VString s3("race conditions are bad");
//  inputVec.push_back({nullptr, &s1});
//  inputVec.push_back({nullptr, &s2});
//  inputVec.push_back({nullptr, &s3});
//  JobState state;
//  JobState last_state={UNDEFINED_STAGE,0};
//  JobHandle job = startMapReduceJob(client, inputVec, outputVec, 4);
//  //getJobState(job, &state);
//
//  while (state.stage != REDUCE_STAGE || state.percentage != 100.0)
//  {
//    if (last_state.stage != state.stage || last_state.percentage != state.percentage){
//      printf("stage %d, %f%% \n",
//             state.stage, state.percentage);
//    }
//    usleep(100000);
//    last_state = state;
//    getJobState(job, &state);
//  }
//  printf("stage %d, %f%% \n",
//         state.stage, state.percentage);
//  printf("Done!\n");
//
//  closeJobHandle(job);
//
//  for (OutputPair& pair: outputVec) {
//    char c = ((const KChar*)pair.first)->c;
//    int count = ((const VCount*)pair.second)->count;
//    printf("The character %c appeared %d time%s\n",
//           c, count, count > 1 ? "s" : "");
//    delete pair.first;
//    delete pair.second;
//  }
//
//  return 0;
//}
//
//
