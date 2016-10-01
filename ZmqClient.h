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

	~ZmqClient() { close(); }

private:
	zmq::context_t ctx;
	zmq::socket_t c_socket;
	std::string topic{"METEO"};
};


#endif //TICKETKAFKA_ZMQCLIENT_H
