
#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>


struct {
    IntermediateVec intermediateVec;
    OutputVec outputVec;
    std::atomic<uint64_t>* atomic_counter;
} typedef ThreadContext;

struct StarterPack {
    const MapReduceClient& client;
    const InputVec& inputVec;
    ThreadContext& t_context;
    std::atomic<uint64_t>* atomic_counter;
} typedef StarterPack;

void* entry_point_map(void* placeholder) {


    StarterPack *starter_pack = static_cast<StarterPack*>(placeholder);
    int num_pairs =  starter_pack->inputVec.size();

    for (; *(starter_pack->atomic_counter) < num_pairs; ++(*(starter_pack->atomic_counter))) {
        K1* key = starter_pack->inputVec.at(*(starter_pack->atomic_counter)).first;
        V1* value = starter_pack->inputVec.at(*(starter_pack->atomic_counter)).second;
        starter_pack->client.map(key, value, &(starter_pack->t_context));
    }
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    pthread_t* threads = new pthread_t[multiThreadLevel];
    ThreadContext* t_contexts = new ThreadContext[multiThreadLevel];
    std::atomic<uint64_t>* atomic_counter = new std::atomic<uint64_t>(0);

    for (int i = 0; i < multiThreadLevel; ++i) {
        IntermediateVec intermediateVec;
        t_contexts[i].atomic_counter = atomic_counter;
        StarterPack starterPack = {client, inputVec, t_contexts[i], atomic_counter};
        pthread_create(threads + i, NULL, entry_point_map, &starterPack);
    }


}


void emit2 (K2* key, V2* value, void* context) { // use atomic_counter
    ThreadContext *t_context = static_cast<ThreadContext*>(context);
    t_context->intermediateVec.push_back(IntermediatePair(key,value));
}

void emit3 (K3* key, V3* value, void* context) {

}


void waitForJob(JobHandle job) {

}

void getJobState(JobHandle job, JobState* state) {

}

void closeJobHandle(JobHandle job) {

}
