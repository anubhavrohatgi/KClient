//
// Created by meox on 24/09/16.
//

#include <mutex>
#include <atomic>
#include <queue>
#include "ZmqServer.h"
#include "zhelpers.hpp"


ZmqServer::ZmqServer()
	: ctx{1}
	, subscriber{ctx, ZMQ_SUB}
	, syncservice{ctx, ZMQ_REQ}
{
	subscriber.connect(c_endpoint);
	subscriber.setsockopt(ZMQ_SUBSCRIBE, "", 0);
	int sndhwm{100000};
	subscriber.setsockopt (ZMQ_SNDHWM, &sndhwm, sizeof (sndhwm));
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
	received = 0;
	std::atomic<bool> stop{false};
	std::queue<std::string> q_data;
	std::mutex m;

	std::thread th_w{[this, &m, &q_data, &stop](){
		std::ofstream f{"out_pubsub.txt"};

		while (!stop)
		{
			std::unique_lock<std::mutex> l(m);
			if (q_data.empty())
			{
				l.unlock();
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
			else
			{
				f << q_data.front() << "\n";
				q_data.pop();
			}
		}

		while(!q_data.empty())
		{
			f << q_data.front() << "\n";
			q_data.pop();
		}
	}};

	while (true)
	{
		const auto msg_str = s_recv(subscriber);
		if (msg_str == "###EXIT###")
			break;

		received++;
		std::unique_lock<std::mutex> l(m);
		q_data.push(msg_str);
	}
	stop = true;
	std::cout << "Exit from main loop (" << received << ")\n";

	th_w.join();

	subscriber.close();
}
