#include <pthread.h>
#include <cstdio>
#include <iostream>
#include <atomic>

#define MT_LEVEL 5

struct ThreadContext {
    std::atomic<int>* atomic_counter;
    int* bad_counter;
};


void* foo(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;
    
    for (int i = 0; i < 1000; ++i) {
        // old_value isn't used in this example, but will be necessary
        // in the exercise
        int old_value = (*(tc->atomic_counter))++;
        (void) old_value;  // ignore not used warning
        (*(tc->bad_counter))++;
    }
    
    return 0;
}


int main(int argc, char** argv)
{
    std::int64_t num = (1 << 31);
    //std::atomic<std::uint64_t> atomic_counter(0);
    //atomic_counter.store(1 << 31);

    std::cout << num << std::endl;

    return 0;
}
