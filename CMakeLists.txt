cmake_minimum_required(VERSION 3.5)
project(TicketKafka)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic-errors -O3 -std=c++14 ${EXTRA_LDFLAGS}")
set(CMAKE_MODULE_PATH "~/cmake/Modules" ${CMAKE_MODULE_PATH})

MESSAGE(STATUS ${CMAKE_MODULE_PATH})
FIND_PACKAGE(LibRDKafka REQUIRED)
FIND_PACKAGE (Threads REQUIRED)
FIND_PACKAGE (ZMQ REQUIRED)

SET(Boost_USE_MULTITHREADED ON)
SET(Boost_USE_STATIC_LIBS ON)
FIND_PACKAGE (Boost 1.59 COMPONENTS filesystem iostreams system REQUIRED)

INCLUDE_DIRECTORIES (${Boost_INCLUDE_DIR})

enable_testing()
find_package(GTest REQUIRED)

set(SOURCE_FILES main.cpp util.h kclient.cpp)

add_executable(TicketKafka ${SOURCE_FILES})
add_executable(TicketKafkaTest test.cpp)

target_link_libraries (TicketKafka ${LibRDKafka_LIBRARIES} ${Boost_FILESYSTEM_LIBRARIES} ${Boost_SYSTEM_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(TicketKafkaTest ${GTEST_BOTH_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})