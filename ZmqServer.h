//
// Created by meox on 24/09/16.
//

#ifndef TICKETKAFKA_ZMQSERVER_H
#define TICKETKAFKA_ZMQSERVER_H

#include <zmq.hpp>
#include <thread>
#include <fstream>
#include <atomic>
#include "util.h"


class ZmqServer
{
public:
	ZmqServer();

	void run();
	void sync_loop();

	~ZmqServer()
	{
		zmq_close(subscriber);
	}

private:
	std::mutex m;
	zmq::context_t ctx;
	zmq::socket_t subscriber;
	zmq::socket_t syncservice;
	bool m_stop;
	std::string c_endpoint{"tcp://127.0.0.1:5560"};
	std::atomic<size_t> received;
};


#endif //TICKETKAFKA_ZMQSERVER_H
