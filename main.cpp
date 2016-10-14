#include <iostream>

#include <boost/filesystem.hpp>
#include <librdkafka/rdkafkacpp.h>
#include <set>
#include <map>
#include <thread>
#include <mutex>
#include <fstream>
#include <sstream>
#include "kclient.h"
#include "ZmqServer.h"
#include "ZmqClient.h"



size_t produce_file(const std::string& fname, KProducer& producer, KTopic& topic, int32_t part)
{
	std::ifstream f{fname};

	size_t n_msg{};
	while (f)
	{
		std::string line;
		std::getline(f, line);

		while(true)
		{
			RdKafka::ErrorCode resp = topic.produce(line, part);
			if (resp == RdKafka::ERR__QUEUE_FULL)
				producer.poll(10);
			else if (resp != RdKafka::ERR_NO_ERROR)
				std::cerr << "> Producer error: " << RdKafka::err2str(resp) << "\n";
			else
				break;
		}
		n_msg++;
	}

	return n_msg;
}

void produce_file(const std::string& fname, std::ofstream& fout)
{
	std::ifstream f{fname};
	std::string line;

	while(f)
	{
		std::getline(f, line);
		fout << line << "\n";
	}
	fout.flush();
}


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


void producer(KClient& client, const std::map<std::string, std::string>& params)
{
	using namespace boost::filesystem;
	try
	{
		size_t n_msg{};
		int32_t part{};
		//std::ofstream f_out{"test_out.txt"};
		KProducer producer = client.create_producer();
		std::cout << "> Created producer " << producer.name() << std::endl;

		// Create topic handle.
		KTopic topic = producer.create_topic(params.at("topic"));
		if (params.find("partition") != params.end())
			part = std::stoi(params.at("partition"));

		//
		path p(params.at("data_in"));
		for (directory_entry& x : directory_iterator(p))
		{
			const auto fname = x.path().string();
			n_msg += produce_file(fname, producer, topic, part);
			//produce_file(x.path().string(), f_out);
			std::cout << "done: " << fname << "\n";
		}

		while (producer.outq_len() > 0)
		{
			std::cout << "Waiting for " << producer.outq_len() << std::endl;
			producer.poll(100);
		}

		std::cout << "Tot message: " << n_msg << "\n";
	}
	catch (std::exception& ex)
	{
		std::cerr << "Error: " << ex.what() << std::endl;
	}
}


void consumer(KClient& client, const std::map<std::string, std::string>& params)
{
	if (params.find("group.id") != params.end())
		client.setGlobalConf("group.id", params.at("group.id"));

	try
	{
		KConsumer consumer = client.create_consumer();
		std::cout << "> Created consumer " << consumer.name() << std::endl;

		consumer.subscribe({params.at("topic")});
		size_t msg_cnt{};
		consumer.for_each(1000, [&msg_cnt](const RdKafka::Message& message){
			//std::cout << "Read msg at offset " << message->offset() << "\n";
			if (message.key())
				std::cout << "Key: " << message.key() << "\n";

			if (message.payload() == nullptr)
				return;
			//std::cout << static_cast<const char *>(message->payload()) << "\n";
			msg_cnt++;
			if (msg_cnt % 1000 == 0)
			{
				std::cout << "*";
				std::flush(std::cout);
			}
		}, [](const RdKafka::Message& message, const RdKafka::ErrorCode err_code){
			if (err_code != RdKafka::ERR__PARTITION_EOF)
			{
				std::cerr << "Error consuming message!\n";
				return false;
			}
			return true;
		});

		std::cout << "\nEnd: " << msg_cnt << "\n";
		consumer.close();
	}
	catch (std::exception& ex)
	{
		std::cerr << "Error: " << ex.what() << std::endl;
	}
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


KClient create_kclient(std::map<std::string, std::string>& params)
{

	/*
	 * Set basic configuration
	 */
	KClient client(params["brokers"]);
	if (!client.setGlobalConf("statistics.interval.ms", "15000"))
		exit(1);

	if (!params["compression"].empty() && !client.setGlobalConf("compression.codec", params.at("compression")))
		exit(1);

	if (!client.setGlobalConf("client.id", params["client.id"]))
		exit(1);


	// load metadata
	if (!client.loadMetadata(params["topic"]))
	{
		std::cerr << "Problem loading metadata\n";
		exit(1);
	}

	return client;
}



int main(int argc, char* argv[])
{
	enum mode_t {PRODUCER, CONUSMER, ZEROMQ_SERVER, ZEROMQ_CLIENT};

	char hostname[128];
	if (gethostname(hostname, sizeof(hostname)))
	{
		std::cerr << "Failed to lookup hostname\n";
		exit(1);
	}

	/*
	 * Read input parameter
	 */

	std::map<std::string, std::string> params;
	for (int i = 0; i < argc; i++)
	{
		if(strcmp(argv[i], "--topic") == 0)
		{
			params["topic"] = argv[i+1];
			i++;
		}
		else if(strcmp(argv[i], "--group-id") == 0)
		{
			params["group.id"] = argv[i+1];
			i++;
		}
		else if(strcmp(argv[i], "--client-id") == 0)
		{
			params["client.id"] = argv[i+1];
			i++;
		}
		else if(strcmp(argv[i], "--mode") == 0)
		{
			params["mode"] = argv[i+1];
			i++;
		}
		else if(strcmp(argv[i], "--brokers") == 0)
		{
			params["brokers"] = argv[i+1];
			i++;
		}
		else if(strcmp(argv[i], "-z") == 0)
		{
			params["compression"] = "snappy";
		}
		else if(strcmp(argv[i], "-p") == 0)
		{
			params["partition"] = argv[i+1];;
			i++;
		}
		else if(strcmp(argv[i], "--data-in") == 0)
		{
			params["data_in"] = argv[i+1];;
			i++;
		}
	}

	if (params["brokers"].empty())
		params["brokers"] = "localhost";

	if (params["topic"].empty())
		params["topic"] = "test";

	if (params["client.id"].empty())
		params["client.id"] = hostname;

	if (params["data_in"].empty())
		params["data_in"] = "/mnt/disk-master/DATA_TX";

	mode_t mode;
	if (params["mode"] == "producer")
		mode = PRODUCER;
	else if (params["mode"] == "consumer")
		mode = CONUSMER;
	else if (params["mode"] == "zmq-server")
		mode = ZEROMQ_SERVER;
	else if (params["mode"] == "zmq-client")
		mode = ZEROMQ_CLIENT;
	else
	{
		std::cerr << "unk mode!\n";
		exit(1);
	}

	switch (mode)
	{
		case PRODUCER:
		{
			auto client = create_kclient(params);
			producer(client, params);
			RdKafka::wait_destroyed(5000);
			break;
		}
		case CONUSMER:
		{
			auto client = create_kclient(params);
			consumer(client, params);
			RdKafka::wait_destroyed(5000);
			break;
		}
		case ZEROMQ_SERVER:
		{
			zmq_server();
			break;
		}
		case ZEROMQ_CLIENT:
		{
			zmq_client();
			break;
		}
	}

	return 0;
}
