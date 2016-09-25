//
// Created by meox on 24/09/16.
//

#include "ZmqServer.h"


void ZmqServer::add_worker()
{
    // start n theads
    for (size_t i = 0; i < 1; i++)
    {
        v_th.emplace_back([this, th_id = i]{
            std::ofstream f{"out_" + std::to_string(th_id) + ".txt"};
            zmq::socket_t socket{ctx, ZMQ_REP};
            socket.connect ("inproc://workerbus");
            std::vector<std::string> buffer;

            while(true)
            {
                zmq::message_t request;
                socket.recv(&request);
                const auto msg_str = kutil::to_string(request);
                socket.send(nullptr, 0);
                if(msg_str == "exit") {
                    break;
                }
                else {
                    buffer.push_back(msg_str);
                }

                if (buffer.size() == 100)
                {
                    for(const auto& x : buffer) {
                        f << x << "\n";
                    }
                    f.flush();
                    buffer.clear();
                }
            }
            for(const auto& x : buffer)
                f << x << "\n";
        });
    }
}
