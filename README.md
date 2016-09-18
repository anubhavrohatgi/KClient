## KClient - Kafka library based on librdkafka 

Tiny modern C++ wrapper for [librdkafka](https://github.com/edenhill/librdkafka) library.

### Compile
    git clone https://github.com/meox/KClient.git
    mkdir build
    cd build
    cmake ..

#### Consumer group example
    //setup client wit the list of boostrap servers
    KClient client("localhost");
    
    //Set basic configuration
    if (!client.setGlobalConf("statistics.interval.ms", "5000"))
        exit(1);

    if (!client.setGlobalConf("client.id", "myapp"))
        exit(1);

    client.setGlobalConf("group.id", "main_consumer");
    
    try
    {
        KConsumer consumer = client.create_consumer();
        std::cout << "> Created consumer " << consumer.name() << std::endl;

        KQueue queue = consumer.create_queue();
        queue.for_each(1000, [](RdKafka::Message& message){
            std::cout << "Read msg at offset " << message.offset() << "\n";
            if (message.key())
                std::cout << "Key: " << *message.key() << "\n";

            std::cout << static_cast<const char *>(message.payload()) << "\n";
        }, [](RdKafka::Message& message){
            std::cerr << "Error reading message or EOF\n";
        }, false);
    }
    catch (std::exception& ex)
    {
        std::cerr << "Error: " << ex.what() << std::endl;
    }

    
#### Producer example
    //setup client wit the list of boostrap servers
    KClient client("localhost");
    
    //Set basic configuration
    if (!client.setGlobalConf("statistics.interval.ms", "5000"))
        exit(1);

    if (!client.setGlobalConf("client.id", "myapp"))
        exit(1);

    try
    {
        KProducer producer = client.create_producer();
        std::cout << "> Created producer " << producer.name() << std::endl;
        
        KTopic topic = producer.create_topic("topic");
        for (size_t i = 0; i < 1000000; i++)
        {
            if (p_it == topic.getPartions().end())
                p_it = topic.getPartions().begin();

            RdKafka::ErrorCode resp = topic.produce("Hello World! " + std::to_string(i), 0);

            if (resp != RdKafka::ERR_NO_ERROR)
            {
                std::cerr << "> Produce failed: " << RdKafka::err2str(resp) << std::endl;
                break;
            }
        }

        while (producer.outq_len() > 0)
        {
            std::cout << "Waiting for " << producer.outq_len() << std::endl;
            producer.poll(1000);
        }
    }
    catch (std::exception& ex)
    {
        std::cerr << "Error: " << ex.what() << std::endl;
    }


### References

- http://docs.confluent.io/3.0.0/clients/
- http://kafka.apache.org/documentation.html#introduction