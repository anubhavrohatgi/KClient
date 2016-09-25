//
// Created by meox on 24/09/16.
//

#include "ZmqServer.h"
#include "zhelpers.hpp"

ZmqServer::ZmqServer()
        : ctx{2}
        , subscriber{ctx, ZMQ_SUB}
{
    subscriber.connect(c_endpoint);
    subscriber.setsockopt(ZMQ_SUBSCRIBE, "METEO", 1);
}


void ZmqServer::run()
{
    std::ofstream f{"out_pubsub.txt"};

    while(true)
    {
        s_recv(subscriber);
        const auto msg_str = s_recv(subscriber);

        if(msg_str == "###EXIT###")
            break;
        else
            f << msg_str << "\n";
    }
    f.flush();
    std::cout << "exit from main loop\n";
    subscriber.close();
}
