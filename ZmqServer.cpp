//
// Created by meox on 24/09/16.
//

#include <mutex>
#include "ZmqServer.h"
#include "zhelpers.hpp"


ZmqServer::ZmqServer()
	: ctx{1}
	, subscriber{ctx, ZMQ_SUB}
	, syncservice{ctx, ZMQ_REQ}
{
	subscriber.connect(c_endpoint);
	subscriber.setsockopt(ZMQ_SUBSCRIBE, "", 0);

	syncservice.connect("tcp://127.0.0.1:5562");
}


void ZmqServer::run()
{
	std::ofstream f{"out_pubsub.txt"};
	size_t c{};

	s_send(syncservice, "READY");
	s_recv(syncservice);

	while (true)
	{
		const auto msg_str = s_recv(subscriber);
		f << msg_str << "\n";

		if (msg_str == "###EXIT###")
			break;

		c++;
	}
	std::cout << "Exit from main loop (" << c << ")\n";

	s_send(syncservice, "DONE");
	s_recv(syncservice);

	subscriber.close();

	f.flush();
}
