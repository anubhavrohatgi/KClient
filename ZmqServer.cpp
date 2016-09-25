//
// Created by meox on 24/09/16.
//

#include "ZmqServer.h"
#include "zhelpers.hpp"


void ZmqServer::run()
{
    std::ofstream f{"out_pubsub.txt"};

    while(true)
    {
        s_recv(subscriber);
        const auto msg_str = s_recv(subscriber);

        if(msg_str == "exit")
            break;
        else
            f << msg_str << "\n";
    }

    std::cout << "exit from main loop\n";
}
