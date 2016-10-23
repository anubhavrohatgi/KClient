cmake_minimum_required(VERSION 3.5)

project(TicketKafka)


### Download dependencies
ADD_SUBDIRECTORY(3rdParty)

find_package(Threads REQUIRED)

enable_testing()

set(SOURCE_FILES main.cpp util.h kclient.cpp ZmqServer.cpp ZmqServer.h ZmqClient.cpp ZmqClient.h util.cpp zmq_util.cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic-errors -std=c++14")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -pedantic-errors -std=c++14")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -pedantic-errors -std=c++14")


# ###############################################
# configure RPATH handling so that it is correctly set both
# for build and install time
# (see http://www.cmake.org/Wiki/CMake_RPATH_handling and http://www.kitware.com/blog/home/post/510)

# enable @rpath in the install name for any shared library being built
SET(CMAKE_MACOSX_RPATH TRUE)

# use, i.e. don't skip the full RPATH for the build tree, this allows running
# executables from the build tree without having to mess with DYLD_LIBRARY_PATH etc..
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
#
# when building, don't use the install RPATH already
# (but later on when installing)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
#
# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
#
#
# the RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
    IF (APPLE)
        SET(CMAKE_INSTALL_RPATH "@executable_path/../lib")
    ELSE(APPLE)
        SET(CMAKE_INSTALL_RPATH "\$ORIGIN/../lib")
    ENDIF()
ENDIF()


ADD_EXECUTABLE (TicketKafka ${SOURCE_FILES})
ADD_EXECUTABLE (TicketKafkaTest test.cpp util.cpp util.h)


ADD_DEPENDENCIES (TicketKafka IMPORTED_LIBZMQ IMPORTED_LIBZMQPP IMPORTED_MSGPACK IMPORTED_BOOST IMPORTED_LIBRDKAFKAPP IMPORTED_LIBRDKAFKA)
ADD_DEPENDENCIES (TicketKafkaTest IMPORTED_LIBZMQ IMPORTED_LIBZMQPP IMPORTED_MSGPACK IMPORTED_BOOST IMPORTED_GTEST)


TARGET_LINK_LIBRARIES (TicketKafka ${Boost_LIBRARY_DIR} boost_system boost_filesystem IMPORTED_LIBZMQ IMPORTED_LIBRDKAFKA IMPORTED_LIBRDKAFKAPP ${CMAKE_THREAD_LIBS_INIT})
TARGET_LINK_LIBRARIES (TicketKafkaTest IMPORTED_GTEST ${Boost_LIBRARY_DIR} IMPORTED_LIBZMQ ${CMAKE_THREAD_LIBS_INIT})

TARGET_INCLUDE_DIRECTORIES(TicketKafka PRIVATE ${CMAKE_BINARY_DIR}/src/LIBZMQPP_DOWNLOAD ${CMAKE_BINARY_DIR}/INSTALL/include)
TARGET_INCLUDE_DIRECTORIES(TicketKafkaTest PRIVATE ${GTEST_INCLUDE_DIR} ${CMAKE_BINARY_DIR}/src/LIBZMQPP_DOWNLOAD)

