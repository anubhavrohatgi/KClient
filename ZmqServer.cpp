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

void ZmqServer::sync_loop()
{
	while (true)
	{
		std::unique_lock<std::mutex> l(m);
		if (m_stop)
			break;
		const std::string s = std::to_string(received);
		l.unlock();

		s_send(syncservice, s);
		s_recv(syncservice);
	}
}


void ZmqServer::run()
{
	std::ofstream f{"out_pubsub.txt"};
	received = 0;
	m_stop = false;

	s_send(syncservice, "READY");
	s_recv(syncservice);
	std::thread th_sync{&ZmqServer::sync_loop, this};

	while (true)
	{
		const auto msg_str = s_recv(subscriber);
		f << msg_str << "\n";

		if (msg_str == "###EXIT###")
			break;

		received++;
	}
	m_stop = true;
	std::cout << "Exit from main loop (" << received << ")\n";

	s_send(syncservice, "DONE");
	s_recv(syncservice);

	th_sync.join();

	subscriber.close();
	f.flush();
}
