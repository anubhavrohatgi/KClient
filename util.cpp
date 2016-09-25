//
// Created by meox on 25/09/16.
//

#include "util.h"


namespace kutil
{

    std::string to_string(const zmq::message_t &msg)
    {
        return std::string(reinterpret_cast<const char*>(msg.data()), msg.size());
    }
}