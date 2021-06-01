cmake_minimum_required(VERSION 3.10)
project(ex3)

set(CMAKE_CXX_STANDARD 11)

include_directories(.)

SET(CMAKE_CXX_FLAGS -pthread)

#add_executable(prog1
#        MapReduceFramework.cpp Atomic/atomicdemo_multivar.cpp Atomic/atomicdemo.cpp
#        Barrier/Barrier.cpp "Sample Client/SampleClient.cpp")

add_executable(prog1
        MapReduceFramework.cpp Atomic/atomicdemo_multivar.cpp Atomic/atomicdemo.cpp
        Barrier/Barrier.cpp "Sample Client/SampleClient.cpp")