cmake_minimum_required(VERSION 3.5)

### Hunter snapshot that will be used ###
include("HunterGate.cmake")
HunterGate(
        URL "https://github.com/ruslo/hunter/archive/v0.15.30.tar.gz"
        SHA1 "0a63c66eebabd7cf5d14962f5527934be7a399a0"
)

project(TicketKafka)

set(CMAKE_MODULE_PATH "~/cmake/Modules" ${CMAKE_MODULE_PATH})
MESSAGE(STATUS ${CMAKE_MODULE_PATH})

find_package(ZeroMQ REQUIRED)
find_package(LibRDKafka REQUIRED)
find_package(Threads REQUIRED)

enable_testing()

### Download dependencies ###
hunter_add_package(GTest)
hunter_add_package(Boost COMPONENTS system filesystem)


### Find dependencies ###
find_package(GTest CONFIG REQUIRED) # GTest::main
find_package(Boost CONFIG REQUIRED system filesystem)

set(SOURCE_FILES main.cpp util.h kclient.cpp ZmqServer.cpp ZmqServer.h ZmqClient.cpp ZmqClient.h)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic-errors -O3 -std=c++14")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -pedantic-errors -O3 -std=c++14")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -pedantic-errors -O3 -std=c++14")

add_executable(TicketKafka ${SOURCE_FILES})
add_executable(TicketKafkaTest test.cpp)

target_link_libraries (TicketKafka ${LibRDKafka_LIBRARIES} Boost::system Boost::filesystem ${ZMQ_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(TicketKafkaTest GTest::main ${CMAKE_THREAD_LIBS_INIT})
