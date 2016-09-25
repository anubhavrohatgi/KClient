//
// Created by meox on 24/09/16.
//

#ifndef TICKETKAFKA_ZMQSERVER_H
#define TICKETKAFKA_ZMQSERVER_H

#include <zmq.hpp>


class ZmqServer
{
public:
    ZmqServer()
    : ctx{3}
    , s_socket{ctx, ZMQ_ROUTER}
    , ipc_socket{ctx, ZMQ_DEALER}
    {
        s_socket.bind(r_endpoint);
    }

    void run()
    {

    }

    void worker()
    {

    }

private:
    zmq::context_t ctx;
    zmq::socket_t s_socket;
    zmq::socket_t ipc_socket;
    std::string r_endpoint{"tcp://*:5559"};
    std::string ipc_endpoint{"ipc://workerbus.ipc"};
};


#endif //TICKETKAFKA_ZMQSERVER_H
