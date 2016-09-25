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
    : ctx{3}
    , clients{ctx, ZMQ_ROUTER}
    , workers{ctx, ZMQ_DEALER}
    {
        clients.bind(c_endpoint);
        workers.bind(w_endpoint);
    }

    void run()
    {
        add_worker();
        zmq::proxy(clients, workers, nullptr);

        for (auto& th : v_th)
            th.join();
    }

    ~ZmqServer()
    {
        zmq_close(clients);
        zmq_close(workers);
    }

protected:
    void add_worker();

private:
    zmq::context_t ctx;
    zmq::socket_t clients;
    zmq::socket_t workers;
    std::string c_endpoint{"tcp://*:5559"};
    std::string w_endpoint{"inproc://workerbus"};
    std::vector<std::thread> v_th;
};


#endif //TICKETKAFKA_ZMQSERVER_H
