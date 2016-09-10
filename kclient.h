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


class EnvConsumeCb : public RdKafka::ConsumeCb
{
public:
    using p_fun_t = std::function<void(RdKafka::Message &message)>;

    EnvConsumeCb(p_fun_t f_msg_cb, p_fun_t f_err_cb)
            : _f_msg_callback{f_msg_cb}
            , _f_err_callback{f_err_cb}
    {}

    void consume_cb(RdKafka::Message &message, void *opaque) override;

private:
    p_fun_t _f_msg_callback;
    p_fun_t _f_err_callback;
};


class KQueue
{
public:
    KQueue(RdKafka::Queue* p_queue) : queue{p_queue}
    {}
    void for_each(int timeout_ms,
                          std::function<void(RdKafka::Message &)> msg_callback,
                          std::function<void(RdKafka::Message &)> error_callback, bool exit_end);

    void setConsumer(RdKafka::Consumer* c) { _consumer = c; }
private:
    RdKafka::Queue* queue{nullptr};
    RdKafka::Producer* _producer{nullptr};
    RdKafka::Consumer* _consumer{nullptr};
};


class KTopic
{
public:
    KTopic(std::string topic_name, RdKafka::Topic *topic, RdKafka::Producer *producer) :
            _topic_name{std::move(topic_name)}, _topic{topic}, _producer{producer}
    {}

    KTopic(std::string topic_name, RdKafka::Topic* topic, RdKafka::Consumer* consumer) :
            _topic_name{std::move(topic_name)}, _topic{topic}, _consumer{consumer}
    {}

    RdKafka::ErrorCode produce(std::string msg, int32_t partition)
    {
        return _producer->produce(_topic, partition,
                                  RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                  const_cast<char *>(msg.c_str()), msg.size(),
                                  NULL, NULL);
    }

    RdKafka::ErrorCode produce(std::string msg)
    {
        return produce(msg, RdKafka::Topic::PARTITION_UA);
    }

    RdKafka::ErrorCode start_consume(int32_t partition, int64_t offset)
    {
        return _consumer->start(_topic, partition, offset);
    }

    RdKafka::ErrorCode stop_consume(int32_t partition)
    {
        return _consumer->stop(_topic, partition);
    }

    RdKafka::ErrorCode stop_consume()
    {
        return stop_consume(RdKafka::Topic::PARTITION_UA);
    }

    RdKafka::Metadata* metadata(int timeout_ms);

    void for_each_part(int32_t partition, int timeout_ms,
                       std::function<void(RdKafka::Message &)> msg_callback,
                       std::function<void(RdKafka::Message &)> error_callback);

    void for_each(uint32_t nth, int timeout_ms,
                  std::function<void(RdKafka::Message &)> msg_callback,
                  std::function<void(RdKafka::Message &)> error_callback);

    ~KTopic() { delete _topic; }

    void setPartionsInfo(const std::set<int32_t> &partions) { _partions = partions; }
    size_t getNumPartions() const { return _partions.size(); }
    const std::set<int32_t>& getPartions() const { return _partions; }

private:
    std::string _topic_name{};
    RdKafka::Topic* _topic{nullptr};
    RdKafka::Producer* _producer{nullptr};
    RdKafka::Consumer* _consumer{nullptr};
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

    ~KProducer() {
        delete _producer;
    }

private:
    RdKafka::Producer* _producer;
    RdKafka::Conf* topic_conf;
    const std::map<std::string, std::set<int32_t>>* map_partions;
};


class KConsumer
{
public:
    KConsumer(RdKafka::Consumer *consumer) : _consumer{consumer}
    {}

    void setTopicConf(RdKafka::Conf *pConf) { topic_conf = pConf; }
    KTopic create_topic(const std::string& topic_str);
    KQueue create_queue();
    std::string name() const { return _consumer->name(); }

    int poll(int timeout_ms) { return _consumer->poll(timeout_ms); }

    ~KConsumer() { delete _consumer; }

    void setPartionsInfo(const std::map<std::string, std::set<int32_t>>* map_part)
    {
        map_partions = map_part;
    }
private:
    RdKafka::Consumer *_consumer;
    RdKafka::Conf *topic_conf;

    const std::map<std::string, std::set<int32_t>>* map_partions;
};


class KClient
{
public:
    KClient()
    {
        conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    }

    KClient(std::string i_brokers)
            : KClient()
    {
        brokers = std::move(i_brokers);
        setGlobalConf("bootstrap.servers", brokers);
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

private:
    RdKafka::Conf *conf;
    RdKafka::Conf *topic_conf;
    std::string brokers;

    std::map<std::string, std::set<int32_t>> map_partions;
    SimplePartionerCb partioner;
};

#endif //TICKETKAFKA_KCLIENT_H
