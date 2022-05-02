
#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include "SampleClient.cpp"


struct {
    int id;
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
    //todo remove
    std::cout << "starting map_entry_point for thread " << starter_pack->t_context.id<<std::endl;
    //std::cout.flush();
    //===========

    int num_pairs =  starter_pack->inputVec.size();

    for (; *(starter_pack->atomic_counter) < num_pairs; ++(*(starter_pack->atomic_counter))) {
        K1* key = starter_pack->inputVec.at(*(starter_pack->atomic_counter)).first;
        V1* value = starter_pack->inputVec.at(*(starter_pack->atomic_counter)).second;
        starter_pack->client.map(key, value, &(starter_pack->t_context));
    }

  //TODO change
  return starter_pack->atomic_counter;
  //===========
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    pthread_t* threads = new pthread_t[multiThreadLevel];
    ThreadContext* t_contexts = new ThreadContext[multiThreadLevel];
    std::atomic<uint64_t>* atomic_counter = new std::atomic<uint64_t>(0);

    for (int i = 0; i < multiThreadLevel; ++i) {
      //TODO remove
        std::cout<<"entering loop for creating thread " << i << std::endl;
      //===========

        IntermediateVec intermediateVec;
        t_contexts[i].atomic_counter = atomic_counter;
        t_contexts[i].id = i;
        StarterPack starterPack = {client, inputVec, t_contexts[i], atomic_counter};
        std::cout<<"creating thread " << i << std::endl;
        pthread_create(threads + i, NULL, entry_point_map, &starterPack);
    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], NULL);
    }
    //todo remove
    for (int i = 0; i < multiThreadLevel; ++i) {
      std::cout << "intermediate_vec for thread " << i << " is: [ ";
      for (int j = 0; j < t_contexts[i].intermediateVec.size(); ++j) {
        std::cout << "(" << t_contexts[i].intermediateVec.at(i).first << " : "
        << t_contexts[i].intermediateVec.at(i).second << ") " << ",";
    }
      std::cout << "]" << std::endl;
  }
    //==========

   //TODO change
  return atomic_counter;
  //=========
}


void emit2 (K2* key, V2* value, void* context) { // use atomic_counter
    ThreadContext *t_context = static_cast<ThreadContext*>(context);
    KChar* kc = static_cast<KChar*>(key);
    VCount* vc = static_cast<VCount*>(value);
    //todo remove
    std::cout<<"starting emit2 for thread " << t_context->id << " with (" << kc->c << "," << vc->count
    << ")" << std::endl;
    //===========

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



int main(int argc, char ** argv){
  CounterClient client;
  InputVec input_vec;
  OutputVec output_vec;
  VString s1("This string is full of characters");
  VString s2("Multithreading is awesome");
  VString s3("race conditions are bad");
  input_vec.push_back({nullptr, &s1});
  input_vec.push_back({nullptr, &s2});
  input_vec.push_back({nullptr, &s3});

  // starting the program
  JobHandle job = startMapReduceJob(client, input_vec, output_vec, 3);


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
