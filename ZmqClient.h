//
// Created by meox on 25/09/16.
//

#ifndef TICKETKAFKA_ZMQCLIENT_H
#define TICKETKAFKA_ZMQCLIENT_H

#include <zmq.hpp>
#include <thread>
#include <chrono>
#include <mutex>


class ZmqClient
{
public:
	ZmqClient();

	void send(const std::string& msg);
	void close();

	void wait_client();
	void sync_loop();

	void stop()
	{
		std::lock_guard<std::mutex> l(m);
		m_stop = true;
	}

	~ZmqClient() { close(); }

private:
	std::mutex m;
	bool m_stop{false};
	size_t sent{}, clinet_rec{};
	zmq::context_t ctx;
	zmq::socket_t c_socket;
	zmq::socket_t syncservice;
};


#endif //TICKETKAFKA_ZMQCLIENT_H
