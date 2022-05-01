cmake_minimum_required(VERSION 3.15)
project(Ex3)

set(CMAKE_CXX_STANDARD 11)

add_library(Ex3 MapReduceClient.h MapReduceFramework.h SampleClient.cpp MapReduceFramework.cpp)