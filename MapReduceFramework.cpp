
#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <algorithm>
#include "SampleClient.cpp"

#define INC_STAGE (long long)1 << 62
#define INC_PROGRESS 1
#define INC_TOTAL(atomic_counter, total) atomic_counter->fetch_add(total << 31)

#define LOAD_STAGE(num) num >> 62
#define LOAD_TOTAL(num) num & 0x3FFFFFFF80000000
#define LOAD_PROGRESS(num) num & 0x7FFFFFFF

struct
{
    int id;
    IntermediateVec intermediateVec;
    OutputVec outputVec;
    std::atomic<uint64_t> *atomic_counter;
} typedef ThreadContext;


struct StarterPack
{
    const MapReduceClient &client;
    const InputVec &inputVec;
    ThreadContext &t_context;
    std::atomic<uint64_t> *atomic_counter;
} typedef StarterPack;

bool comparePairs(IntermediatePair a, IntermediatePair b) {
    return *(a.first) < *(b.first);
}

void *entry_point_map(void *placeholder)
{

    StarterPack *starter_pack = static_cast<StarterPack *>(placeholder);
    //todo remove
    //std::cout << "starting map_entry_point for thread " << starter_pack->t_context.id <<
    // std::endl;
    //===========

    int num_pairs = starter_pack->inputVec.size();
    int old_value = LOAD_PROGRESS((starter_pack->atomic_counter)->fetch_add(INC_PROGRESS));
    while (old_value < num_pairs)
    {
        K1 *key = starter_pack->inputVec.at(old_value).first;
        V1 *value = starter_pack->inputVec.at(old_value).second;
        starter_pack->client.map(key, value, &(starter_pack->t_context));
        old_value = LOAD_PROGRESS((starter_pack->atomic_counter)->fetch_add(INC_PROGRESS));
    }


    auto vec_begin = starter_pack->t_context.intermediateVec.begin();
    auto vec_end = starter_pack->t_context.intermediateVec.end();
    std::sort(vec_begin, vec_end, comparePairs);
    //TODO change
    return starter_pack->atomic_counter;
    //===========
}


JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    pthread_t *threads = new pthread_t[multiThreadLevel];
    ThreadContext *t_contexts = new ThreadContext[multiThreadLevel];
    std::atomic<uint64_t> *atomic_counter = new std::atomic<uint64_t>(0);
    INC_TOTAL(atomic_counter, inputVec.size());
    atomic_counter->fetch_add(INC_STAGE);
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        IntermediateVec intermediateVec;
        t_contexts[i].atomic_counter = atomic_counter;
        t_contexts[i].id = i;

        StarterPack *starterPack = new StarterPack{client, inputVec, t_contexts[i], atomic_counter};

        pthread_create(threads + i, NULL, entry_point_map, starterPack);
    }

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_join(threads[i], NULL);
    }


    //todo remove
    for (int i = 0; i < multiThreadLevel; ++i)
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
    }
    //==========


    //TODO change
    return atomic_counter;
    //=========
}


void emit2(K2 *key, V2 *value, void *context)
{ // use atomic_counter
    ThreadContext *t_context = static_cast<ThreadContext *>(context);
    t_context->intermediateVec.push_back(IntermediatePair(key, value));
}


void emit3(K3 *key, V3 *value, void *context)
{

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
    VString s3("race conditions are bad");
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
