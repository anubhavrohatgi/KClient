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
		s_send(syncservice, "OK");

		{
			std::lock_guard<std::mutex> l(m);
			clinet_rec = std::stoul(c_str);
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
		const auto diff = sent - clinet_rec;
		//std::cout << "d " << diff << "\n";
		l.unlock();

		if (diff < 10000)
			break;

		//std::cout << "diff = " << diff << "\n";
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
