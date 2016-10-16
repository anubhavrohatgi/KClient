//
// Created by meox on 04/09/16.
//

#ifndef TICKETKAFKA_KCLIENT_H
#define TICKETKAFKA_KCLIENT_H

#include "util.h"
#include <mutex>
#include <thread>
#include <map>
#include <set>
#include <memory>
#include <functional>
#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <future>


class SimplePartionerCb : public RdKafka::PartitionerCb {
public:
	using p_fun_t = std::function<int32_t(const RdKafka::Topic *topic, const std::string *key, int32_t partition_cnt, void *msg_opaque)>;
	int32_t partitioner_cb (const RdKafka::Topic *topic, const std::string *key,
							int32_t partition_cnt, void *msg_opaque) override {
		return p_fun(topic, key, partition_cnt, msg_opaque);
	}

	void setCallBack(p_fun_t f)
	{
		p_fun = move(f);
	}
private:
	p_fun_t p_fun;
};


class KRebalanceCb : public RdKafka::RebalanceCb
{
public:
	KRebalanceCb() = default;
	KRebalanceCb(KRebalanceCb&& rhs)
	: partition_cnt(rhs.partition_cnt)
	, eof_partition(rhs.eof_partition)
	, f_rebalance(rhs.f_rebalance)
	{}

	void rebalance_cb (RdKafka::KafkaConsumer *consumer,
					   RdKafka::ErrorCode err,
					   std::vector<RdKafka::TopicPartition*> &partitions);

	size_t get_partition_cnt() const { return partition_cnt; }
	size_t get_eof_partition() const { return eof_partition; }

	void inc_eof_partion() { eof_partition++; }
	void reset_eof_partion() { eof_partition = 0; }

	void print() const
	{
		std::cerr << "partition_cnt: " << partition_cnt << ", eof_partition = " << eof_partition << std::endl;
	}

	bool is_rebalanced() const
	{
		std::lock_guard<std::mutex> l{m};
		return f_rebalance;
	}

private:
	void part_list_print (const std::vector<RdKafka::TopicPartition*>&partitions)
	{
		for (unsigned int i = 0 ; i < partitions.size() ; i++)
			std::cerr << partitions[i]->topic() <<
					  "[" << partitions[i]->partition() << "], ";
		std::cerr << "\n";
	}

	size_t partition_cnt{};
	size_t eof_partition{};

	mutable std::mutex m;
	bool f_rebalance{false};
};


class KTopic
{
public:
	KTopic(std::string topic_name, RdKafka::Topic *topic, RdKafka::Producer *producer) :
			_topic_name{std::move(topic_name)}, _topic{topic}, _producer{producer}
	{}

	KTopic(std::string topic_name, RdKafka::Topic* topic, RdKafka::KafkaConsumer* consumer) :
			_topic_name{std::move(topic_name)}, _topic{topic}, _consumer{consumer}
	{}

	RdKafka::ErrorCode produce(const std::string& msg, int32_t partition)
	{
		return _producer->produce(_topic, partition,
								  RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
								  const_cast<char *>(msg.c_str()), msg.size(),
								  NULL, NULL);
	}

	bool sync_produce(const std::string& msg, int32_t partition)
	{
		while(true)
		{
			RdKafka::ErrorCode resp = produce(msg, partition);
			if (resp == RdKafka::ERR__QUEUE_FULL)
				_producer->poll(10);
			else if (resp != RdKafka::ERR_NO_ERROR)
			{
				std::cerr << "> Producer error: " << RdKafka::err2str(resp) << "\n";
				return false;
			}
			else
				return true;
		}
		return false;
	}

	RdKafka::Metadata* metadata(int timeout_ms);

	~KTopic() { delete _topic; }

	void setPartionsInfo(const std::set<int32_t> &partions) { _partions = partions; }
	size_t getNumPartions() const { return _partions.size(); }
	const std::set<int32_t>& getPartions() const { return _partions; }

private:
	std::string _topic_name{};
	RdKafka::Topic* _topic{nullptr};
	RdKafka::Producer* _producer{nullptr};
	RdKafka::KafkaConsumer* _consumer{nullptr};
	std::set<int32_t> _partions;
};


class KProducer
{
public:
	KProducer(RdKafka::Producer *producer) : _producer{producer}
	{}

	KTopic create_topic(const std::string& topic_str);

	std::string name() const { return _producer->name(); }
	void setTopicConf(RdKafka::Conf *pConf) { topic_conf = pConf; }
	int outq_len() const { return _producer->outq_len(); }
	int poll(int timeout) const { return _producer->poll(timeout); }

	void setPartionsInfo(const std::map<std::string, std::set<int32_t>>* map_part)
	{
		map_partions = map_part;
	}

	~KProducer() { delete _producer; }

private:
	RdKafka::Producer* _producer{nullptr};
	RdKafka::Conf* topic_conf{nullptr};
	const std::map<std::string, std::set<int32_t>>* map_partions{nullptr};
};


struct message_raii{
	RdKafka::Message* data{nullptr};
	~message_raii()
	{
		delete data;
	}
};

class KConsumer
{
public:
	KConsumer(RdKafka::KafkaConsumer *consumer) : _consumer{consumer}
	{}

	void setTopicConf(RdKafka::Conf *pConf) { topic_conf = pConf; }
	KTopic create_topic(const std::string& topic_str);

	void subscribe(const std::vector<std::string>& topics)
	{
		_consumer->subscribe(topics);
	}

	RdKafka::Message* consume(int time_out)
	{
		return _consumer->consume(time_out);
	}


	void commit()
	{
		_consumer->commitSync();
	}

	void reset_eof_partion()
	{
		rebalance_cb->reset_eof_partion();
	}

	void wait_rebalance()
	{
		while (!rebalance_cb->is_rebalanced())
		{
			_consumer->poll(100);
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	}

	template<typename F, typename E>
	void for_each(int time_out, F && f, E && err)
	{
		while(true)
		{
			message_raii message;
			message.data = consume(time_out);

			if (message.data == nullptr)
				break;

			const auto msg_err = message.data->err();
			switch (message.data->err())
			{
				case RdKafka::ERR__TIMED_OUT:
				{
					//std::cerr << "Timeout" << std::endl;
					break;
				}
				case RdKafka::ERR_NO_ERROR:
				{
					/* Real message */
					f(*message.data);
					break;
				}
				case RdKafka::ERR__PARTITION_EOF:
				{
					rebalance_cb->inc_eof_partion();
					rebalance_cb->print();
					if (rebalance_cb->get_eof_partition() == rebalance_cb->get_partition_cnt()
							&& err(*message.data, msg_err)) {
						return;
					}
					break;
				}

				case RdKafka::ERR__UNKNOWN_TOPIC:
				case RdKafka::ERR__UNKNOWN_PARTITION:
				{
					std::cerr << "Consume failed: " << message.data->errstr() << std::endl;
					if (err(*message.data, msg_err))
						return;
					break;
				}
				default:
				{
					/* Errors */
					std::cerr << "Consume failed: " << message.data->errstr() << std::endl;
					if (err(*message.data, msg_err))
						return;
				}
			}
		}
	}

	std::string name() const { return _consumer->name(); }

	int poll(int timeout_ms) { return _consumer->poll(timeout_ms); }

	~KConsumer() { delete _consumer; }

	void setRebalanceCb(KRebalanceCb* rebalance)
	{
		rebalance_cb = rebalance;
	}

	void close();

private:
	RdKafka::KafkaConsumer* _consumer{nullptr};
	RdKafka::Conf* topic_conf{nullptr};
	KRebalanceCb* rebalance_cb;
};


class KEventCb : public RdKafka::EventCb
{
public:
	void event_cb (RdKafka::Event &event);
private:
	bool run{false};
};


class KClient
{
public:
	KClient()
	{
		conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
		topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	}

	KClient(KClient&& rhs)
			: event_cb(std::move(rhs.event_cb))
			, rebalance_cb(std::move(rhs.rebalance_cb))
			, conf{rhs.conf}
			, topic_conf{rhs.topic_conf}
			, brokers(std::move(rhs.brokers))
			, map_partions(std::move(rhs.map_partions))
	{
	}

	KClient(std::string i_brokers)
	: KClient()
	{
		brokers = std::move(i_brokers);
		setGlobalConf("bootstrap.servers", brokers);
		setGlobalConf("metadata.broker.list", brokers);

		setConf(conf, "event_cb", &event_cb);
		setConf(conf, "rebalance_cb", &rebalance_cb);

		default_topic_conf();
	}

	bool setGlobalConf(const std::string& param, const std::string& val)
	{
		return setConf(conf, param, val);
	}

	bool setTopicConf(const std::string& param, const std::string& val)
	{
		return setConf(topic_conf, param, val);
	}

	bool setPartioner(RdKafka::PartitionerCb& partioner);
	bool setPartioner(SimplePartionerCb::p_fun_t fun)
	{
		partioner.setCallBack(fun);
		return setPartioner(partioner);
	}

	KProducer create_producer();

	KConsumer create_consumer();
	bool loadMetadata(const std::string& topic_str = "");

	void default_topic_conf();

protected:
	bool setConf(RdKafka::Conf * p_conf, const std::string& param, const std::string& val)
	{
		if (!p_conf)
			return false;

		std::string errstr;
		if (p_conf->set(param, val, errstr) != RdKafka::Conf::CONF_OK)
		{
			std::cerr << errstr << std::endl;
			return false;
		}

		return true;
	}

	template <typename T>
	bool setConf(RdKafka::Conf * p_conf, const std::string& param, T* val)
	{
		if (!p_conf)
			return false;

		std::string errstr;
		if (p_conf->set(param, val, errstr) != RdKafka::Conf::CONF_OK)
		{
			std::cerr << errstr << std::endl;
			return false;
		}

		return true;
	}

private:
	KEventCb event_cb;
	KRebalanceCb rebalance_cb;

	RdKafka::Conf *conf;
	RdKafka::Conf *topic_conf;
	std::string brokers;

	std::map<std::string, std::set<int32_t>> map_partions;
	SimplePartionerCb partioner;
};

#endif //TICKETKAFKA_KCLIENT_H
