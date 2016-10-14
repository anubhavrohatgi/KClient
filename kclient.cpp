//
// Created by meox on 04/09/16.
//

#include "util.h"
#include <mutex>
#include <thread>
#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include "kclient.h"


void KQueue::for_each(int timeout_ms,
					std::function<void(const RdKafka::Message &)> msg_callback,
					std::function<void(const RdKafka::Message &)> error_callback,
					bool exit_end)
{
	EnvConsumeCb env_cb(msg_callback, error_callback);
	while (true)
	{
		bool params[] = {true, false};
		while (params[0])
		{
			RdKafka::Message* msg = _consumer->consume(timeout_ms);
			env_cb.consume_cb(*msg, &params);
			_consumer->poll(30);
		}

		if (exit_end || params[1])
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


void EnvConsumeCb::consume_cb(const RdKafka::Message& message, void *opaque) {
	bool *op = reinterpret_cast<bool*>(opaque);
	bool *run = &op[0];
	bool *end = &op[1];

	switch (message.err())
	{
		case RdKafka::ERR__TIMED_OUT:
			*run = false;
			//*end = true;
			return;

		case RdKafka::ERR_NO_ERROR:
			/* Real message */
			_f_msg_callback(message);
			break;

		case RdKafka::ERR__PARTITION_EOF:
			/* Last message */
			_f_err_callback(message);
			*run = false;
			*end = true;
			return;

		case RdKafka::ERR__UNKNOWN_TOPIC:
		case RdKafka::ERR__UNKNOWN_PARTITION:
			std::cerr << "Consume failed: " << message.errstr() << std::endl;
			*end = true;
			return;

		default:
			/* Errors */
			std::cerr << "Consume failed: " << message.errstr() << std::endl;
			*run = false;
			_f_err_callback(message);
	}
}


KConsumer KClient::create_consumer()
{
	std::string errstr;
	RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
	if (!consumer)
		throw std::runtime_error("Failed to create consumer: " + errstr);

	KConsumer kconsumer{consumer};
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

	return true;
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

KQueue KConsumer::create_queue(const std::string& topic)
{
	return create_queue(std::vector<std::string>{topic});
}

KQueue KConsumer::create_queue(const std::vector<std::string>& topics)
{
	KQueue q{RdKafka::Queue::create(_consumer)};

	q.setConsumer(_consumer);
	_consumer->subscribe(topics);
	return q;
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
			std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
					  event.str() << std::endl;
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
				run = false;
			break;

		case RdKafka::Event::EVENT_STATS:
			std::cerr << "\"STATS\": " << event.str() << std::endl;
			break;

		case RdKafka::Event::EVENT_LOG:
			fprintf(stderr, "LOG-%i-%s: %s\n",
					event.severity(), event.fac().c_str(), event.str().c_str());
			break;

		case RdKafka::Event::EVENT_THROTTLE:
			std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
					  event.broker_name() << " id " << (int)event.broker_id() << std::endl;
			break;

		default:
			std::cerr << "EVENT " << event.type() <<
					  " (" << RdKafka::err2str(event.err()) << "): " <<
					  event.str() << std::endl;
			break;
	}
}
