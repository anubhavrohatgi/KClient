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
	int sndhwm{100000};
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
	zmq::message_t zmsg{msg.c_str(), msg.size()};
	while(!c_socket.send(zmsg))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	sent++;
}

void ZmqClient::wait_client()
{
	s_recv(syncservice);
	s_send(syncservice, "OK");
}
