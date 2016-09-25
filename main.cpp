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
#include <zmq.hpp>


void produce_file(const std::string& fname, KProducer& producer, KTopic& topic)
{
    std::ifstream f{fname};

    while (f)
    {
        std::string line;
        std::getline(f, line);

        while(true)
        {
            RdKafka::ErrorCode resp = topic.produce(line, 0);
            if (resp == RdKafka::ERR__QUEUE_FULL)
                producer.poll(10);
            else if (resp != RdKafka::ERR_NO_ERROR)
                std::cerr << "> Producer error: " << RdKafka::err2str(resp) << "\n";
            else
                break;
        }
    }
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


void produce_file(const std::string& fname, ZmqClient& pub)
{
    std::ifstream f{fname};

    while (!f.eof())
    {
        std::string line{};
        std::getline(f, line);
        pub.send(line);
    }
}


void producer(KClient& client, const std::map<std::string, std::string>& params)
{
    using namespace boost::filesystem;
    try
    {
        std::ofstream f_out{"test_out.txt"};
        KProducer producer = client.create_producer();
        std::cout << "> Created producer " << producer.name() << std::endl;

        // Create topic handle.
        KTopic topic = producer.create_topic(params.at("topic"));

        //
        path p("/mnt/disk-master/DATA_TX");
        for (directory_entry& x : directory_iterator(p))
        {
            const auto fname = x.path().string();
            produce_file(fname, producer, topic);
            //produce_file(x.path().string(), f_out);
            std::cout << "done: " << fname << "\n";
        }

        while (producer.outq_len() > 0)
        {
            std::cout << "Waiting for " << producer.outq_len() << std::endl;
            producer.poll(100);
        }
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

    bool exit_end{false};
    if (params.find("exit_end") != params.end() && params.at("exit_end") == "true")
        exit_end = true;

    try
    {
        KConsumer consumer = client.create_consumer();
        std::cout << "> Created consumer " << consumer.name() << std::endl;

        std::vector<double> temp_avg;
        KQueue queue = consumer.create_queue(params.at("topic"));

        queue.for_each(500, [&temp_avg](const RdKafka::Message& message){
            std::cout << "Read msg at offset " << message.offset() << "\n";
            if (message.key())
                std::cout << "Key: " << *message.key() << "\n";

            /*std::stringstream smsg;
            smsg << static_cast<const char *>(message.payload());
            std::string s;
            double temp;
            smsg >> s >> s >> temp;
            std::cout << temp << "\n";
            temp_avg.push_back(temp);*/

            std::cout << message.payload() << std::endl;

        }, [](const RdKafka::Message& message){
            std::cerr << "Error reading message or EOF\n";
        }, exit_end);

        double sum = std::accumulate(temp_avg.begin(), temp_avg.end(), 0.0, [](double a, double b){
           return a+b;
        });

        if (!temp_avg.empty())
            std::cout << "size: " << temp_avg.size() << ", temp Avg: " << sum/(double)temp_avg.size() << "\n";
        else
            std::cout << "no data!\n";

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
    ZmqClient zmqClient;

    path p("/mnt/disk-master/DATA_TX");
    for (directory_entry& x : directory_iterator(p))
    {
        const auto fname = x.path().string();
        produce_file(fname, zmqClient);
        std::cout << "done: " << fname << "\n";
    }
    zmqClient.send("###EXIT###");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    zmqClient.close();
}


KClient create_kclient(std::map<std::string, std::string>& params)
{

    /*
     * Set basic configuration
     */
    KClient client(params["brokers"]);
    if (!client.setGlobalConf("statistics.interval.ms", "5000"))
        exit(1);

    if (!params["compression"].empty() && !client.setGlobalConf("compression.codec", "snappy"))
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
            i++;
        }
    }

    if (params["brokers"].empty())
        params["brokers"] = "localhost";

    if (params["topic"].empty())
        params["topic"] = "test";

    if (params["client.id"].empty())
        params["client.id"] = hostname;


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
            break;
        }
        case CONUSMER:
        {
            auto client = create_kclient(params);
            consumer(client, params);
            break;
        }
        case ZEROMQ_SERVER:
            zmq_server();
            break;
        case ZEROMQ_CLIENT:
            zmq_client();
            break;
    }

    RdKafka::wait_destroyed(5000);

    return 0;
}
