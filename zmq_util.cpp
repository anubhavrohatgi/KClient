//
// Created by meox on 15/10/16.
//

#include "ZmqServer.h"
#include "ZmqClient.h"
#include <boost/filesystem.hpp>
#include <iostream>


size_t produce_file(const std::string& fname, ZmqClient& pub)
{
	std::ifstream f{fname};
	std::string line;
	size_t c{};

	while(f)
	{
		std::getline(f, line, '\n');
		pub.send(line);
		c++;
	}

	return c;
}

void zmq_server()
{
	ZmqServer zmqServer;
	zmqServer.run();
}


void zmq_client()
{
	using namespace boost::filesystem;

	size_t c{};
	ZmqClient zmq_client;
	path p("/mnt/disk-master/DATA_TX");

	zmq_client.wait_client();
	std::thread th_sync{&ZmqClient::sync_loop, &zmq_client};

	for (directory_entry& x : directory_iterator(p))
	{
		time_t t_s, t_e;
		const auto fname = x.path().string();
		time(&t_s);
		const auto n = produce_file(fname, zmq_client);
		time(&t_e);
		c += n;
		std::cout << "done: " << fname << ", #nmsg = " << c << " - " << (double)n / (double)(t_e - t_s) << " msg/s\n";
	}

	zmq_client.send("###EXIT###");
	zmq_client.wait_client();

	zmq_client.stop();
	th_sync.join();
	zmq_client.close();

	std::cout << "Messages produces: " << c << "\n";
}
