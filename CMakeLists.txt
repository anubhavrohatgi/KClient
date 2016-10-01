cmake_minimum_required(VERSION 3.5)

project(TicketKafka)

set(CMAKE_MODULE_PATH "~/cmake/Modules" ${CMAKE_MODULE_PATH})
MESSAGE(STATUS ${CMAKE_MODULE_PATH})


### Download dependencies ###
ADD_SUBDIRECTORY(3rdParty)

find_package(LibRDKafka REQUIRED)
find_package(Threads REQUIRED)

enable_testing()

set(SOURCE_FILES main.cpp util.h kclient.cpp ZmqServer.cpp ZmqServer.h ZmqClient.cpp ZmqClient.h util.cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic-errors -std=c++14")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -pedantic-errors -std=c++14")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -pedantic-errors -std=c++14")

add_executable(TicketKafka ${SOURCE_FILES})
add_executable(TicketKafkaTest test.cpp util.cpp util.h)

target_link_libraries (TicketKafka ${LibRDKafka_LIBRARIES} ${IMPORTED_BOOST} ${ZMQ_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(TicketKafkaTest ${IMPORTED_GTEST} ${ZMQ_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

ADD_DEPENDENCIES (TicketKafka IMPORTED_LIBZMQ IMPORTED_LIBZMQPP IMPORTED_MSGPACK IMPORTED_BOOST)
ADD_DEPENDENCIES (TicketKafkaTest IMPORTED_LIBZMQ IMPORTED_LIBZMQPP IMPORTED_MSGPACK IMPORTED_BOOST IMPORTED_GTEST)
