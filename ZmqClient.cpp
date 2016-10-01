//
// Created by meox on 25/09/16.
//

#include "ZmqClient.h"
#include "zhelpers.hpp"


ZmqClient::ZmqClient()
	: ctx{1}
	, c_socket{ctx, ZMQ_PUB}
	, syncservice{ctx, ZMQ_REP}
{
	c_socket.bind("tcp://*:5560");
	int sndhwm{};
	c_socket.setsockopt (ZMQ_SNDHWM, &sndhwm, sizeof (sndhwm));
	syncservice.bind("tcp://*:5562");
}

void ZmqClient::sync_loop()
{
	while (true)
	{
		{
			std::lock_guard<std::mutex> l(m);
			if (m_stop)
				break;
		}

		const auto c_str = s_recv(syncservice);
		s_send(syncservice, "");

		{
			std::lock_guard<std::mutex> l(m);
			client_rec = std::stoul(c_str);
		}
	}
}


void ZmqClient::close()
{
	c_socket.close();
}


void ZmqClient::send(const std::string &msg)
{
	while (true)
	{
		std::unique_lock<std::mutex> l(m);
		if (client_rec == 0)
		{
			l.unlock();
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
			break;
		}

		const auto diff = sent - client_rec;
		if (diff < 10000)
			break;

		l.unlock();
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
	}

	s_send(c_socket, msg);
	sent++;
}

void ZmqClient::wait_client()
{
	s_recv(syncservice);
	s_send(syncservice, "OK");
}
