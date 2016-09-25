//
// Created by meox on 25/09/16.
//

#include "ZmqClient.h"

ZmqClient::ZmqClient(const std::string &ip)
        : ctx{3}
        , c_socket{ctx, ZMQ_REQ}
{
    c_socket.connect("tcp://" + ip + ":5559");
}

void ZmqClient::send(const std::string &msg)
{
    zmq::message_t req;
    c_socket.send(msg.c_str(), msg.size());
    c_socket.recv(&req);
}

