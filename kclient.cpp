//
// Created by meox on 04/09/16.
//

#include "util.h"
#include <mutex>
#include <thread>
#include <map>
#include <set>
#include <memory>
#include <functional>
#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include "kclient.h"


void KTopic::for_each_part(int32_t partition, int timeout_ms,
                   std::function<void(RdKafka::Message &)> msg_callback,
                   std::function<void(RdKafka::Message &)> error_callback)
{
    EnvConsumeCb env_cb(msg_callback, error_callback);

    bool run{true};
    while (run)
    {
        _consumer->consume_callback(_topic, partition, timeout_ms, &env_cb, &run);
        _consumer->poll(0);
    }
}


void KTopic::for_each(uint32_t nth, int timeout_ms,
              std::function<void(RdKafka::Message &)> msg_callback,
              std::function<void(RdKafka::Message &)> error_callback)
{
    std::mutex m;
    std::vector<std::thread> th_v;

    const auto p_part = kutil::partions(_partions, nth);
    for (const auto& vpar : p_part)
    {
        th_v.push_back(std::thread([this, vpar, timeout_ms, &m, &msg_callback, &error_callback]{
            EnvConsumeCb env_cb(msg_callback, error_callback);

            bool run{true};
            while (run)
            {
                std::lock_guard<std::mutex> l(m);
                for (auto par : vpar)
                    _consumer->consume_callback(_topic, par, timeout_ms, &env_cb, &run);
                _consumer->poll(10);
            }
        }));
    }

    for (auto& th : th_v)
        th.join();
}


void KQueue::for_each(int timeout_ms,
                      std::function<void(RdKafka::Message &)> msg_callback,
                      std::function<void(RdKafka::Message &)> error_callback, bool exit_end)
{
    EnvConsumeCb env_cb(msg_callback, error_callback);
    while (true)
    {
        bool run{true};
        while (run)
        {
            _consumer->consume_callback(queue, timeout_ms, &env_cb, &run);
            _consumer->poll(10);
        }

        if (exit_end)
            break;

        std::this_thread::sleep_for(std::chrono::duration<unsigned int, std::milli>(100));
    }
}


RdKafka::Metadata* KTopic::metadata(int timeout_ms)
{
    RdKafka::Metadata* metadata;

    /* Fetch metadata */
    RdKafka::ErrorCode err = _producer->metadata(_topic != nullptr, _topic, &metadata, timeout_ms);
    if (err != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "Failed to acquire metadata: " << RdKafka::err2str(err) << std::endl;
        return nullptr;
    }

    return metadata;
}


void EnvConsumeCb::consume_cb(RdKafka::Message &message, void *opaque) {
    bool *run = reinterpret_cast<bool*>(opaque);

    switch (message.err())
    {
        case RdKafka::ERR__TIMED_OUT:
            break;

        case RdKafka::ERR_NO_ERROR:
            /* Real message */
            _f_msg_callback(message);
            break;

        case RdKafka::ERR__PARTITION_EOF:
            /* Last message */
            _f_err_callback(message);
            *run  = false;
            break;

        case RdKafka::ERR__UNKNOWN_TOPIC:
        case RdKafka::ERR__UNKNOWN_PARTITION:
            std::cerr << "Consume failed: " << message.errstr() << std::endl;
            *run  = false;
            break;

        default:
            /* Errors */
            std::cerr << "Consume failed: " << message.errstr() << std::endl;
            *run  = false;
            _f_err_callback(message);
    }
    _f_msg_callback(message);
}

KConsumer KClient::create_consumer()
{
    std::string errstr;
    RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
    if (!consumer) {
        throw std::runtime_error("Failed to create consumer: " + errstr);
    }

    auto kconsumer = KConsumer(consumer);
    kconsumer.setTopicConf(topic_conf);
    kconsumer.setPartionsInfo(&map_partions);
    return kconsumer;
}

bool KClient::loadMetadata(const std::string &topic_str)
{
    KProducer p = create_producer();
    KTopic topic = p.create_topic(topic_str);

    RdKafka::Metadata* metadata = topic.metadata(50000);
    if (metadata == nullptr)
        return false;

    for (const RdKafka::TopicMetadata* tm : *metadata->topics())
    {
        std::cout << "found topic: " << tm->topic() << "\n";
        auto& p_set = map_partions[tm->topic()];
        for (const RdKafka::PartitionMetadata* pm : *tm->partitions())
        {
            p_set.insert(pm->id());
            std::cout << "p: " << pm->id() << "\n";
        }
    }

    return false;
}

KProducer KClient::create_producer()
{
    std::string errstr;
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        throw std::runtime_error("Failed to create producer: " + errstr);
    }

    auto kproducer = KProducer(producer);
    kproducer.setTopicConf(topic_conf);

    return kproducer;
}

bool KClient::setPartioner(RdKafka::PartitionerCb &partioner)
{
    if (!topic_conf)
        return false;

    std::string errstr;
    if (topic_conf->set("partitioner_cb", &partioner, errstr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << errstr << std::endl;
        return false;
    }

    return true;
}

template <typename T>
KTopic create_topic(const std::string &topic_str, T* h, RdKafka::Conf* topic_conf, const std::map<std::string, std::set<int32_t>>* map_partions)
{
    std::string errstr;
    RdKafka::Topic* topic{nullptr};

    if (!topic_str.empty())
    {
        topic = RdKafka::Topic::create(h, topic_str, topic_conf, errstr);
        if (!topic) {
            throw std::runtime_error("Failed to create topic: "  + errstr);
        }
    }

    KTopic ktopic{topic_str, topic, h};
    if (map_partions != nullptr)
        ktopic.setPartionsInfo(map_partions->at(topic_str));

    return ktopic;
}

KTopic KProducer::create_topic(const std::string &topic_str)
{
    return ::create_topic(topic_str, _producer, topic_conf, map_partions);
}

KTopic KConsumer::create_topic(const std::string &topic_str)
{
    return ::create_topic(topic_str, _consumer, topic_conf, map_partions);
}

KQueue KConsumer::create_queue()
{
    KQueue q{RdKafka::Queue::create(_consumer)};
    q.setConsumer(_consumer);
    return q;
}

