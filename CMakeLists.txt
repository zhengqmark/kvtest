cmake_minimum_required(VERSION 3.5)
project(KVTest)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}")
set(CMAKE_CXX_STANDARD 11)

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

find_package(Storelib MODULE REQUIRED)

add_executable(KVTest0 kv_test0.cc)
target_link_libraries(KVTest Storelib Threads::Threads)
