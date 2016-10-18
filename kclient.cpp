//
// Created by meox on 04/09/16.
//

#include "util.h"
#include <mutex>
#include <thread>
#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include "kclient.h"


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

KConsumer KClient::create_consumer()
{
	std::string errstr;
	RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
	if (!consumer)
		throw std::runtime_error("Failed to create consumer: " + errstr);

	KConsumer kconsumer{consumer};
	setConf(topic_conf, "offset.store.method", "broker");
	setConf(topic_conf, "auto.commit.enable", "true");

	kconsumer.setTopicConf(topic_conf);
	kconsumer.setRebalanceCb(&rebalance_cb);
	return kconsumer;
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

void KClient::default_topic_conf()
{
	std::string errstr;
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	setConf(tconf, "auto.commit.enable", "true");

	//setConf(tconf, "auto.offset.reset", "latest");
	/* Consumer groups always use broker based offset storage */
	setConf(tconf, "offset.store.method", "broker");

	conf->set("default_topic_conf", tconf, errstr);
	std::cout << errstr << std::endl;
}


template <typename T>
KTopic create_topic(const std::string &topic_str, T* h, RdKafka::Conf* topic_conf)
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
	return ktopic;
}

KTopic KProducer::create_topic(const std::string &topic_str)
{
	return ::create_topic(topic_str, _producer, topic_conf);
}

KTopic KConsumer::create_topic(const std::string &topic_str)
{
	return ::create_topic(topic_str, _consumer, topic_conf);
}


void KConsumer::close()
{
	_consumer->close();
}


void KEventCb::event_cb(RdKafka::Event &event)
{
	kutil::print_time();
	switch (event.type())
	{
		case RdKafka::Event::EVENT_ERROR:
		{
			std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
					  event.str() << std::endl;
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
				run = false;
			break;
		}
		case RdKafka::Event::EVENT_STATS:
		{
			std::cerr << "\"STATS\": " << event.str() << std::endl;
			break;
		}
		case RdKafka::Event::EVENT_LOG:
		{
			fprintf(stderr, "LOG-%i-%s: %s\n",
					event.severity(), event.fac().c_str(), event.str().c_str());
			break;
		}
		case RdKafka::Event::EVENT_THROTTLE:
		{
			std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
					  event.broker_name() << " id " << (int)event.broker_id() << std::endl;
			break;
		}
		default:
		{
			std::cerr << "EVENT " << event.type() <<
					  " (" << RdKafka::err2str(event.err()) << "): " <<
					  event.str() << std::endl;
			break;
		}
	}
}


void KRebalanceCb::rebalance_cb(RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err,
								std::vector<RdKafka::TopicPartition *> &partitions)
{
	std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

	part_list_print(partitions);

	if (err == RdKafka::ERR__ASSIGN_PARTITIONS)
	{
		consumer->assign(partitions);
		partition_cnt = partitions.size();
		std::lock_guard<std::mutex> l{m};
		f_rebalance = true;
	}
	else
	{
		consumer->unassign();
		partition_cnt = 0;
	}
	eof_partition = 0;
	print();
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

	return true;
}