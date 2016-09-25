//
// Created by meox on 25/09/16.
//

#include "ZmqClient.h"
#include "zhelpers.hpp"

ZmqClient::ZmqClient()
        : ctx{1}
        , c_socket{ctx, ZMQ_PUB}
{
    c_socket.bind("tcp://*:5560");
}

void ZmqClient::send(const std::string &msg)
{
    s_sendmore(c_socket, topic);
    s_send(c_socket, msg);
}

