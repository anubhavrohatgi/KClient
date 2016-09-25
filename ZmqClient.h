//
// Created by meox on 25/09/16.
//

#ifndef TICKETKAFKA_ZMQCLIENT_H
#define TICKETKAFKA_ZMQCLIENT_H

#include <zmq.hpp>


class ZmqClient
{
public:
    ZmqClient(const std::string& ip);

    void send(const std::string& msg);

private:
    zmq::context_t ctx;
    zmq::socket_t c_socket;
};


#endif //TICKETKAFKA_ZMQCLIENT_H
