//
// Created by meox on 24/09/16.
//

#ifndef TICKETKAFKA_ZMQSERVER_H
#define TICKETKAFKA_ZMQSERVER_H

#include <zmq.hpp>
#include <thread>
#include <fstream>
#include "util.h"


class ZmqServer
{
public:
    ZmqServer()
    : ctx{1}
    , subscriber{ctx, ZMQ_SUB}
    {
        subscriber.connect(c_endpoint);
        subscriber.setsockopt(ZMQ_SUBSCRIBE, "METEO", 1);
    }

    void run();

    ~ZmqServer()
    {
        zmq_close(subscriber);
    }

private:
    zmq::context_t ctx;
    zmq::socket_t subscriber;
    std::string c_endpoint{"tcp://127.0.0.1:5560"};
};


#endif //TICKETKAFKA_ZMQSERVER_H
