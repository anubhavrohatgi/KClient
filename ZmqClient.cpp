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


void ZmqClient::close()
{
	c_socket.close();
}


void ZmqClient::send(const std::string &msg)
{
	s_send(c_socket, msg);
}

void ZmqClient::wait_client()
{
	s_recv(syncservice);
	s_send(syncservice, "OK");
}
