//
// Created by meox on 24/09/16.
//

#include <mutex>
#include "ZmqServer.h"
#include "zhelpers.hpp"


ZmqServer::ZmqServer()
	: ctx{2}
	, subscriber{ctx, ZMQ_SUB}
{
	subscriber.setsockopt(ZMQ_SUBSCRIBE, "METEO", 1);
	subscriber.connect(c_endpoint);
}


void ZmqServer::run()
{
	std::ofstream f{"out_pubsub.txt"};
	size_t c{};

	while (true)
	{
		s_recv(subscriber);
		const auto msg_str = s_recv(subscriber);
		f << msg_str << "\n";
		c++;

		if (c == 86376047)
			break;
	}

	f.flush();
	std::cout << "\n\nExit from main loop (" << c << ")\n";

	subscriber.close();
}
