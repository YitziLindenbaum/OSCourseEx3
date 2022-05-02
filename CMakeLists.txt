cmake_minimum_required(VERSION 3.15)
project(Ex3)

set(CMAKE_CXX_STANDARD 11)

add_library(Ex3 MapReduceClient.h MapReduceFramework.h SampleClient.cpp MapReduceFramework.cpp)
add_executable(ex3_exe MapReduceFramework.cpp SampleClient.cpp)
target_link_libraries(ex3_exe Ex3)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")