//
// Created by meox on 25/09/16.
//

#ifndef TICKETKAFKA_ZMQCLIENT_H
#define TICKETKAFKA_ZMQCLIENT_H

#include <zmq.hpp>
#include <thread>
#include <chrono>


class ZmqClient
{
public:
	ZmqClient();

	void send(const std::string& msg);
	void close();

	void wait_client();

	~ZmqClient() { close(); }

private:
	zmq::context_t ctx;
	zmq::socket_t c_socket;
	zmq::socket_t syncservice;
};


#endif //TICKETKAFKA_ZMQCLIENT_H
